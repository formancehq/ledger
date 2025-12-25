#!/bin/bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
# Get script directory (where this script is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LEDGER_URL="${LEDGER_URL:-http://localhost:9000}"
BENCH_DURATION="${BENCH_DURATION:-30s}"
BENCH_PARALLELISM="${BENCH_PARALLELISM:-5}"
BENCH_SCRIPT="${BENCH_SCRIPT:-cmd/bench/scripts/any_unbounded_to_any.js}"
RESULTS_DIR="${RESULTS_DIR:-${PROJECT_ROOT}/build/storage-comparison-results}"
PROFILES_DIR="${RESULTS_DIR}/profiles"
DEBUG="${DEBUG:-false}"

# Helper functions (must be defined before use)
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Extract storage drivers from openapi.yml
extract_drivers_from_openapi() {
    local openapi_file="${PROJECT_ROOT}/openapi.yml"
    
    # Try to use yq if available (more reliable for YAML parsing)
    if command -v yq >/dev/null 2>&1; then
        yq eval '.components.schemas.CreateLedgerRequest.properties.driver.enum[]' "${openapi_file}" 2>/dev/null | tr '\n' ' '
        return $?
    fi
    
    # Fallback: use grep and sed to extract enum values
    # Look for the enum definition in CreateLedgerRequest.driver
    # Pattern: enum: [sqlite-mattn, sqlite-modern]
    local enum_line=$(grep -A 10 "CreateLedgerRequest:" "${openapi_file}" | grep -A 5 "driver:" | grep "enum:" | head -1)
    
    if [ -n "${enum_line}" ]; then
        # Extract values between [ and ], then clean them up
        # Remove leading/trailing spaces, replace commas with spaces
        echo "${enum_line}" | sed -n 's/.*enum: \[\(.*\)\].*/\1/p' | sed 's/,/ /g' | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//' | sed 's/[[:space:]]\+/ /g'
    else
        echo "sqlite-mattn sqlite-modern"  # Fallback defaults
        return 1
    fi
}

# Get storage drivers from openapi.yml
STORAGE_DRIVERS_RAW=$(extract_drivers_from_openapi)
if [ -z "${STORAGE_DRIVERS_RAW}" ]; then
    log_warning "Could not extract drivers from openapi.yml, using defaults"
    STORAGE_DRIVERS=("sqlite-mattn" "sqlite-modern")
else
    # Convert space-separated string to array
    read -ra STORAGE_DRIVERS <<< "${STORAGE_DRIVERS_RAW}"
fi

log_info "Found ${#STORAGE_DRIVERS[@]} storage driver(s): ${STORAGE_DRIVERS[*]}"

# Check if docker-compose is running
check_docker_compose() {
    log_info "Checking if docker-compose is running..."
    
    if docker-compose ps | grep -q "Up"; then
        log_success "Docker-compose is already running"
        return 0
    else
        log_warning "Docker-compose is not running, starting it..."
        # Unset DEBUG to avoid polluting docker-compose output
        DEBUG_SAVED="${DEBUG:-}"
        unset DEBUG
        docker-compose up -d > /dev/null 2>&1
        # Restore DEBUG if it was set
        if [ -n "${DEBUG_SAVED}" ]; then
            export DEBUG="${DEBUG_SAVED}"
        fi
        
        log_info "Waiting for services to be healthy..."
        local max_attempts=300 # Cross compilation can be very long in docker
        local attempt=0
        
        while [ $attempt -lt $max_attempts ]; do
            if curl -sf "${LEDGER_URL}/health" > /dev/null 2>&1; then
                log_success "Services are healthy"
                return 0
            fi
            attempt=$((attempt + 1))
            sleep 2
        done
        
        log_error "Services did not become healthy in time"
        return 1
    fi
}

# Wait for ledger to be ready
wait_for_ledger() {
    local ledger_name=$1
    local max_attempts=30
    local attempt=0
    
    log_info "Waiting for ledger '${ledger_name}' to be ready..."
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -sf "${LEDGER_URL}/${ledger_name}" > /dev/null 2>&1; then
            log_success "Ledger '${ledger_name}' is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    
    log_error "Ledger '${ledger_name}' did not become ready in time"
    return 1
}

# Get client command (using go run)
get_client_command() {
    echo "go run"
}

# Create a ledger
create_ledger() {
    local ledger_name=$1
    local driver=$2
    
    log_info "Creating ledger '${ledger_name}' with driver '${driver}'..."
    
    local client_cmd=$(get_client_command)
    
    # Use CLI client with --name and --driver flags to avoid interactive prompts
    if [ "${DEBUG}" = "true" ]; then
        (cd "${PROJECT_ROOT}" && ${client_cmd} ./cmd/client ledgers create \
            --server="${LEDGER_URL}" \
            --name="${ledger_name}" \
            --driver="${driver}" \
            --metadata="{}" \
            --no-metadata) 2>&1
    else
        (cd "${PROJECT_ROOT}" && ${client_cmd} ./cmd/client ledgers create \
            --server="${LEDGER_URL}" \
            --name="${ledger_name}" \
            --driver="${driver}" \
            --metadata="{}" \
            --no-metadata) > /dev/null 2>&1
    fi
    
    if [ $? -eq 0 ]; then
        log_success "Ledger '${ledger_name}' created successfully"
        wait_for_ledger "${ledger_name}"
        return 0
    else
        log_error "Failed to create ledger '${ledger_name}'"
        return 1
    fi
}

# Delete a ledger
delete_ledger() {
    local ledger_name=$1
    
    log_info "Deleting ledger '${ledger_name}'..."
    
    local client_cmd=$(get_client_command)
    
    # Use CLI client with --name flag to avoid interactive prompts
    # The client will skip confirmation when name is provided via flag
    if [ "${DEBUG}" = "true" ]; then
        if (cd "${PROJECT_ROOT}" && ${client_cmd} ./cmd/client ledgers delete \
            --server="${LEDGER_URL}" \
            --name="${ledger_name}") 2>&1; then
            log_success "Ledger '${ledger_name}' deleted successfully"
            return 0
        else
            log_error "Failed to delete ledger '${ledger_name}'"
            return 1
        fi
    else
        if (cd "${PROJECT_ROOT}" && ${client_cmd} ./cmd/client ledgers delete \
            --server="${LEDGER_URL}" \
            --name="${ledger_name}") > /dev/null 2>&1; then
            log_success "Ledger '${ledger_name}' deleted successfully"
            return 0
        else
            log_error "Failed to delete ledger '${ledger_name}'"
            return 1
        fi
    fi
}

# Clean data between benchmarks for precise results
clean_data() {
    log_info "Cleaning data before benchmark..."
    
    # Save and unset DEBUG to avoid polluting docker-compose output and affecting services
    DEBUG_SAVED="${DEBUG:-}"
    unset DEBUG
    
    # Stop docker-compose services
    log_info "Stopping docker-compose services..."
    docker-compose stop > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        log_warning "Failed to stop docker-compose services, continuing..."
    fi
    
    # Clean data using just clean
    log_info "Running 'just clean' to clean data..."
    (cd "${PROJECT_ROOT}" && just clean) > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        log_warning "Failed to run 'just clean', continuing..."
    fi
    
    # Small delay to ensure everything is stopped and cleaned before restarting
    log_info "Waiting 2 seconds before restarting services..."
    sleep 2
    
    # Restart docker-compose services
    log_info "Restarting docker-compose services..."
    docker-compose up -d > /dev/null 2>&1
    
    if [ $? -ne 0 ]; then
        log_error "Failed to restart docker-compose services"
        # Restore DEBUG before returning
        if [ -n "${DEBUG_SAVED}" ]; then
            export DEBUG="${DEBUG_SAVED}"
        fi
        return 1
    fi
    
    # Restore DEBUG if it was set
    if [ -n "${DEBUG_SAVED}" ]; then
        export DEBUG="${DEBUG_SAVED}"
    fi
    
    # Wait for services to be healthy
    log_info "Waiting for services to be healthy..."
    local max_attempts=300
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if curl -sf "${LEDGER_URL}/health" > /dev/null 2>&1; then
            log_success "Services are healthy"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    log_error "Services did not become healthy in time"
    return 1
}

# Run benchmark
run_benchmark() {
    local ledger_name=$1
    local driver=$2
    local cpu_profile_file="${PROFILES_DIR}/${driver}-cpu.prof"
    
    log_info "Running benchmark for ledger '${ledger_name}' (driver: ${driver})..."
    log_info "Duration: ${BENCH_DURATION}, Parallelism: ${BENCH_PARALLELISM}"
    
    # Resolve benchmark script path
    local bench_script_path="${BENCH_SCRIPT}"
    if [[ ! "${bench_script_path}" =~ ^/ ]]; then
        # Relative path, resolve from project root
        bench_script_path="${PROJECT_ROOT}/${bench_script_path}"
    fi
    
    # Run benchmark with CPU profiling using go run
    if [ "${DEBUG}" = "true" ]; then
        (cd "${PROJECT_ROOT}" && go run ./cmd/bench \
            --ledger.url="${LEDGER_URL}" \
            --ledger.name="${ledger_name}" \
            --duration="${BENCH_DURATION}" \
            --parallelism="${BENCH_PARALLELISM}" \
            --script="${bench_script_path}" \
            --cpu-profile.file="${cpu_profile_file}" \
            --report.file="${RESULTS_DIR}/${driver}-report.json") 2>&1 || {
            log_error "Benchmark failed for driver '${driver}'"
            return 1
        }
    else
        (cd "${PROJECT_ROOT}" && go run ./cmd/bench \
            --ledger.url="${LEDGER_URL}" \
            --ledger.name="${ledger_name}" \
            --duration="${BENCH_DURATION}" \
            --parallelism="${BENCH_PARALLELISM}" \
            --script="${bench_script_path}" \
            --cpu-profile.file="${cpu_profile_file}" \
            --report.file="${RESULTS_DIR}/${driver}-report.json") > /dev/null 2>&1 || {
            log_error "Benchmark failed for driver '${driver}'"
            return 1
        }
    fi
    
    log_success "Benchmark completed for driver '${driver}'"
    return 0
}

# Compare CPU profiles
compare_cpu_profiles() {
    log_info "Comparing CPU profiles..."
    
    if [ ${#STORAGE_DRIVERS[@]} -lt 2 ]; then
        log_warning "Need at least 2 storage drivers to compare"
        return 0
    fi
    
    local profiles=()
    for driver in "${STORAGE_DRIVERS[@]}"; do
        local profile_file="${PROFILES_DIR}/${driver}-cpu.prof"
        if [ -f "${profile_file}" ]; then
            profiles+=("${profile_file}")
        else
            log_warning "CPU profile file not found: ${profile_file}"
        fi
    done
    
    if [ ${#profiles[@]} -lt 2 ]; then
        log_warning "Not enough CPU profile files to compare"
        return 0
    fi
    
    log_info "Generating CPU comparison report..."
    
    # Use pprof to compare profiles
    # First profile is the baseline
    local baseline="${profiles[0]}"
    local comparison_file="${RESULTS_DIR}/cpu-profile-comparison.txt"
    
    echo "=== CPU Profile Comparison ===" > "${comparison_file}"
    echo "Baseline: ${baseline}" >> "${comparison_file}"
    echo "" >> "${comparison_file}"
    
    for i in $(seq 1 $((${#profiles[@]} - 1))); do
        local compare_to="${profiles[$i]}"
        echo "--- Comparing ${baseline} vs ${compare_to} ---" >> "${comparison_file}"
        go tool pprof -top -cum -base="${baseline}" "${compare_to}" >> "${comparison_file}" 2>&1 || true
        echo "" >> "${comparison_file}"
    done
    
    log_success "CPU comparison report written to: ${comparison_file}"
    
    # Also generate web comparison if possible
    log_info "To view interactive CPU comparison, run:"
    echo "  go tool pprof -http=:8080 -base=${baseline} ${profiles[1]}"
}


# Main execution
main() {
    log_info "Starting storage comparison benchmark"
    log_info "LEDGER_URL: ${LEDGER_URL}"
    log_info "BENCH_DURATION: ${BENCH_DURATION}"
    log_info "BENCH_PARALLELISM: ${BENCH_PARALLELISM}"
    log_info "BENCH_SCRIPT: ${BENCH_SCRIPT}"
    if [ "${DEBUG}" = "true" ]; then
        log_info "DEBUG mode: enabled (output will be shown)"
    fi
    
    # Create results directory
    rm -rf ${RESULTS_DIR}
    mkdir -p "${PROFILES_DIR}"
    
    # Test each storage driver
    local failed_drivers=()
    
    for driver in "${STORAGE_DRIVERS[@]}"; do
        local ledger_name="bench-${driver}-$(date +%s)"
        
        log_info "=========================================="
        log_info "Testing storage driver: ${driver}"
        log_info "=========================================="
        
        # Clean data before each benchmark for precise results
        if ! clean_data; then
            log_error "Failed to clean data before benchmark for driver '${driver}'"
            failed_drivers+=("${driver}")
            continue
        fi
        
        # Create ledger
        if ! create_ledger "${ledger_name}" "${driver}"; then
            log_error "Failed to create ledger for driver '${driver}'"
            failed_drivers+=("${driver}")
            continue
        fi
        
        # Run benchmark
        if ! run_benchmark "${ledger_name}" "${driver}"; then
            log_error "Benchmark failed for driver '${driver}'"
            failed_drivers+=("${driver}")
            # Try to delete ledger even if benchmark failed
            delete_ledger "${ledger_name}" || true
            continue
        fi
        
        # Delete ledger
        if ! delete_ledger "${ledger_name}"; then
            log_warning "Failed to delete ledger '${ledger_name}', but continuing..."
        fi
        
        # Small delay between tests
        sleep 2
    done
    
    # Compare CPU profiles
    compare_cpu_profiles
    
    # Generate HTML viewer with auto-loaded reports
    log_info "Generating benchmark viewer with auto-loaded reports..."
    local viewer_script="${SCRIPT_DIR}/benchmark-viewer.html"
    local viewer_output="${RESULTS_DIR}/benchmark-viewer.html"
    
    if [ ! -f "${viewer_script}" ]; then
        log_warning "Benchmark viewer script not found: ${viewer_script}"
        return
    fi
    
    # Find all report JSON files
    local report_files=()
    while IFS= read -r -d '' file; do
        report_files+=("${file}")
    done < <(find "${RESULTS_DIR}" -maxdepth 1 -name "*-report.json" -type f -print0 2>/dev/null)
    
    if [ ${#report_files[@]} -eq 0 ]; then
        log_warning "No report files found, copying viewer without auto-load"
        cp "${viewer_script}" "${viewer_output}"
        return
    fi
    
    # Generate JavaScript array of report paths (relative to HTML file)
    local report_paths_js="["
    local first=true
    for report_file in "${report_files[@]}"; do
        local filename=$(basename "${report_file}")
        if [ "$first" = true ]; then
            first=false
        else
            report_paths_js+=", "
        fi
        report_paths_js+="\"${filename}\""
    done
    report_paths_js+="]"
    
    # Inject report paths into HTML
    cp "${viewer_script}" "${viewer_output}"
    
    # Inject report paths using a more reliable method
    # Read the file, replace the line, write it back
    local temp_file=$(mktemp)
    local found=false
    
    while IFS= read -r line; do
        if [[ "$line" =~ "window.reportPaths = window.reportPaths ||" ]] && [ "$found" = false ]; then
            echo "        window.reportPaths = ${report_paths_js};"
            found=true
        else
            echo "$line"
        fi
    done < "${viewer_output}" > "${temp_file}"
    
    if [ "$found" = true ]; then
        mv "${temp_file}" "${viewer_output}"
    else
        rm -f "${temp_file}"
        log_warning "Could not inject report paths, using manual file selection"
    fi
    
    log_success "Benchmark viewer generated at: ${viewer_output}"
    log_info "Found ${#report_files[@]} report file(s)"
    log_info "Open ${viewer_output} in your browser to view results (reports will auto-load)"
    
    # Summary
    log_info "=========================================="
    log_info "Benchmark Summary"
    log_info "=========================================="
    
    if [ ${#failed_drivers[@]} -eq 0 ]; then
        log_success "All storage drivers tested successfully"
    else
        log_error "Failed drivers: ${failed_drivers[*]}"
    fi
    
    log_info "Results directory: ${RESULTS_DIR}"
    log_info "Profile files: ${PROFILES_DIR}"
    
    # List generated files
    log_info "Generated files:"
    ls -lh "${RESULTS_DIR}" || true
    
    log_info ""
    log_success "Benchmark viewer: ${viewer_output}"
    log_info "Open ${viewer_output} in your browser to visualize all results"
}

# Run main function
main "$@"

