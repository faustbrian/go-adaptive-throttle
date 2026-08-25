#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
manifest="${root}/migration/standalone/repositories.json"
destination_root="${GOLIB_STANDALONE_DESTINATION_ROOT:-/Users/brian/Developer/golib}"
selected_module="${GOLIB_STANDALONE_MUTATION_MODULE:-}"
task="$(mktemp -d -t golib-mutation-bootstrap.XXXXXX)"

cleanup() {
    find "${task}" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

command -v jq >/dev/null
command -v zip >/dev/null

export GOCACHE="${task}/gocache"
export GOMODCACHE="${task}/gomodcache"
export GONOSUMDB="github.com/faustbrian/go-*"
mkdir -p "${GOCACHE}" "${GOMODCACHE}" "${task}/proxy"
go build -o "${task}/golib" ./cmd/golib
"${task}/golib" standalone-proxy \
    --destination-root "${destination_root}" \
    --output "${task}/proxy"
export GOPROXY="file://${task}/proxy,https://proxy.golang.org,direct"

expected_total="$(jq -r \
    --arg selected "${selected_module}" '
        if $selected == "" then .counts.releasable_modules
        else ([.modules[] | select(.releasable and .module_path == $selected)] | length)
        end
    ' "${manifest}")"
[[ "${expected_total}" -gt 0 ]]
module_total=0
checkpoint_total=0

while IFS=$'\t' read -r repository source_module module module_path; do
    destination="${destination_root}/${repository}"
    source_checkpoints="${root}/.artifacts/${source_module}/mutation-checkpoints"
    artifact=root
    [[ "${module}" == "." ]] || artifact="${module//\//-}"
    staging="${task}/${repository}/${artifact}/mutation-checkpoints"
    archive="${destination}/.golib/mutation-bootstrap/${artifact}.zip"
    ledger="${destination}/.golib/mutation-history-migrations.json"

    [[ -d "${source_checkpoints}" && -s "${ledger}" ]]
    mkdir -p "${staging}" "$(dirname "${archive}")"

    while IFS=$'\t' read -r package slug; do
        source_checkpoint="${source_checkpoints}/${slug}.json"
        staged_checkpoint="${staging}/${slug}.json"
        [[ -s "${source_checkpoint}" ]]
        jq -e \
            --arg source_module "${source_module}" \
            --arg package "${package}" '
                .schema_version == 3 and
                .module == $source_module and
                .package == $package and
                (.report.files | type == "array") and
                ([.report.files[].mutations[]? | select(.status != "KILLED")] | length == 0)
            ' "${source_checkpoint}" >/dev/null

        execution_revision="$(jq -er '.execution_revision' "${source_checkpoint}")"
        previous_digest="$(jq -er '.gate_input_digest' "${source_checkpoint}")"
        gremlins_version="$(jq -er '.gremlins_version' "${source_checkpoint}")"
        report_digest="$({ jq -S -c '.report' "${source_checkpoint}" || exit 1; } |
            shasum -a 256 | awk '{print $1}')"

        checkpoint_verifier="$(jq -r '.gremlins_verifier_sha256 // empty' \
            "${source_checkpoint}")"
        verifier_source="$(jq -r '.verifier_identity_source // empty' \
            "${source_checkpoint}")"
        if [[ "${verifier_source}" == "executed" &&
            "${checkpoint_verifier}" =~ ^[0-9a-f]{64}$ ]]; then
            current_verifier="$(GOLIB_ROOT="${destination}" \
                "${destination}/.golib/scripts/mutation-verifier-identity.sh")"
            [[ "${checkpoint_verifier}" == "${current_verifier}" ]]
            verifier_identity="${checkpoint_verifier}"
            jq \
                --arg module "${module}" \
                --arg package "${package}" \
                --arg execution_revision "${execution_revision}" \
                --arg previous_digest "${previous_digest}" \
                --arg gremlins_version "${gremlins_version}" \
                --arg verifier_identity "${verifier_identity}" \
                --arg report_digest "${report_digest}" '
                    .verifier_migrations = (
                        [.verifier_migrations[] | select(
                            .module != $module or
                            .package != $package or
                            .execution_revision != $execution_revision or
                            .gate_input_digest != $previous_digest
                        )] + [{
                            module: $module,
                            package: $package,
                            execution_revision: $execution_revision,
                            gate_input_digest: $previous_digest,
                            gremlins_version: $gremlins_version,
                            gremlins_verifier_sha256: $verifier_identity,
                            report_sha256: $report_digest
                        }]
                    )
                ' "${ledger}" >"${task}/ledger.json"
            mv "${task}/ledger.json" "${ledger}"
        else
            verifier_identity="$(jq -er \
                --arg module "${module}" \
                --arg package "${package}" \
                --arg execution_revision "${execution_revision}" \
                --arg previous_digest "${previous_digest}" \
                --arg gremlins_version "${gremlins_version}" \
                --arg report_digest "${report_digest}" '
                    . as $ledger
                    | def digest_ancestry($digest):
                        [$digest] + (
                            [
                                $ledger.entries[]
                                | select(
                                    .module == $module and
                                    .package == $package and
                                    .execution_revision == $execution_revision and
                                    .replacement_gate_input_digest == $digest and
                                    .gremlins_version == $gremlins_version and
                                    .report_sha256 == $report_digest
                                )
                                | digest_ancestry(.gate_input_digest)
                            ] | add // []
                        );
                    [
                        digest_ancestry($previous_digest)[] as $digest
                        | $ledger.verifier_migrations[]
                        | select(
                            .module == $module and
                            .package == $package and
                            .execution_revision == $execution_revision and
                            .gate_input_digest == $digest and
                            .gremlins_version == $gremlins_version and
                            .report_sha256 == $report_digest
                        )
                        | .gremlins_verifier_sha256
                    ]
                    | unique
                    | if length == 1 then .[0]
                      else error("verifier identity ancestry is not unique")
                      end
                ' "${ledger}")"
        fi

        [[ "${verifier_identity}" =~ ^[0-9a-f]{64}$ ]]
        if [[ "${checkpoint_verifier}" =~ ^[0-9a-f]{64}$ ]]; then
            [[ "${checkpoint_verifier}" == "${verifier_identity}" ]]
        fi

        replacement_digest="$(
            GOLIB_ROOT="${destination}" \
                "${destination}/.golib/scripts/gate-input-digest.sh" \
                mutation "${module}" "${package}"
        )"
        jq \
            --arg module "${module}" \
            --arg package "${package}" \
            --arg execution_revision "${execution_revision}" \
            --arg previous_digest "${previous_digest}" \
            --arg replacement_digest "${replacement_digest}" \
            --arg gremlins_version "${gremlins_version}" \
            --arg verifier_identity "${verifier_identity}" \
            --arg report_digest "${report_digest}" '
                .entries = (
                    [.entries[] | select(
                        .module != $module or
                        .package != $package or
                        .execution_revision != $execution_revision or
                        .gate_input_digest != $previous_digest
                    )] + [{
                        module: $module,
                        package: $package,
                        execution_revision: $execution_revision,
                        gate_input_digest: $previous_digest,
                        replacement_gate_input_digest: $replacement_digest,
                        gremlins_version: $gremlins_version,
                        gremlins_verifier_sha256: $verifier_identity,
                        report_sha256: $report_digest
                    }]
                )
            ' "${ledger}" >"${task}/ledger.json"
        mv "${task}/ledger.json" "${ledger}"

        jq --arg module "${module}" '.module = $module' \
            "${source_checkpoint}" >"${staged_checkpoint}"
        touch -t 200001010000 "${staged_checkpoint}"
        checkpoint_total=$((checkpoint_total + 1))
    done < <(
        jq -r --arg path "${module_path}" '
            .modules[]
            | select(.module_path == $path)
            | .packages[]
            | select(.coverage_required == true)
            | [.directory, (if .directory == "." then "root" else (.directory | gsub("/"; "-")) end)]
            | @tsv
        ' "${destination}/modules.json" | LC_ALL=C sort
    )

    archive_tmp="${task}/${repository}/${artifact}.zip"
    (
        cd "${task}/${repository}/${artifact}"
        LC_ALL=C find mutation-checkpoints -type f -print | LC_ALL=C sort |
            zip -X -q "${archive_tmp}" -@
    )
    mv "${archive_tmp}" "${archive}"
    unzip -tqq "${archive}"
    module_total=$((module_total + 1))
done < <(
    jq -r --arg selected "${selected_module}" '
        .modules[]
        | select(.releasable == true and ($selected == "" or .module_path == $selected))
        | [.repository, .source_directory, .directory, .module_path]
        | @tsv
    ' "${manifest}"
)

[[ "${module_total}" -eq "${expected_total}" ]]
if [[ -z "${selected_module}" ]]; then
    [[ "${checkpoint_total}" -eq 554 ]]
else
    [[ "${checkpoint_total}" -gt 0 ]]
fi
printf 'migrated %d mutation checkpoints for %d standalone modules\n' \
    "${checkpoint_total}" "${module_total}"
