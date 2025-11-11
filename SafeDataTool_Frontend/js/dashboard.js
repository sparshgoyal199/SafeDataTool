const API_BASE_URL = "http://127.0.0.1:8000";
const TOKEN_KEY = "safedata_token";

const state = {
    datasets: [],
    configs: [],
    runs: [],
    selectedRunId: null,
};

document.addEventListener("DOMContentLoaded", () => {
    const token = getAuthToken();
    if (!token) {
        window.location.href = "./login.html";
        return;
    }

    setupEventHandlers();
    refreshAll();
});

function setupEventHandlers() {
    document.getElementById("dataset-upload-form")?.addEventListener("submit", handleDatasetUpload);
    document.getElementById("config-form")?.addEventListener("submit", handleConfigCreate);
    document.getElementById("run-form")?.addEventListener("submit", handleRunExecute);
    document.getElementById("privacy-technique")?.addEventListener("change", toggleTechniqueParameters);
    document.getElementById("refresh-button")?.addEventListener("click", refreshAll);
    document.getElementById("logout-button")?.addEventListener("click", () => {
        localStorage.removeItem(TOKEN_KEY);
        window.location.href = "./login.html";
    });
}

function getAuthToken() {
    return localStorage.getItem(TOKEN_KEY);
}

async function fetchWithAuth(path, options = {}) {
    const token = getAuthToken();
    const headers = new Headers(options.headers || {});
    if (token) {
        headers.set("Authorization", `Bearer ${token}`);
    }
    const response = await fetch(`${API_BASE_URL}${path}`, { ...options, headers });
    if (response.status === 401) {
        showToast("Session expired. Please log in again.", "error");
        localStorage.removeItem(TOKEN_KEY);
        setTimeout(() => (window.location.href = "./login.html"), 1200);
        throw new Error("Unauthorized");
    }
    return response;
}

function toggleTechniqueParameters() {
    const selected = document.getElementById("privacy-technique").value;
    document.querySelectorAll(".parameter-group").forEach((group) => {
        const technique = group.getAttribute("data-technique");
        if (technique === selected) {
            group.classList.remove("hidden");
        } else {
            group.classList.add("hidden");
        }
    });
}

async function handleDatasetUpload(event) {
    event.preventDefault();
    const name = document.getElementById("dataset-name").value.trim();
    const description = document.getElementById("dataset-description").value.trim();
    const fileInput = document.getElementById("dataset-file");

    if (!fileInput.files.length) {
        showToast("Please select a dataset file.", "error");
        return;
    }

    const formData = new FormData();
    formData.append("name", name);
    formData.append("description", description);
    formData.append("file", fileInput.files[0]);

    try {
        const response = await fetchWithAuth("/datasets", {
            method: "POST",
            body: formData,
        });

        if (!response.ok) {
            const error = await safeJson(response);
            throw new Error(error?.message || "Failed to upload dataset");
        }

        showToast("Dataset uploaded successfully.", "success");
        event.target.reset();
        await loadDatasets();
    } catch (err) {
        showToast(err.message || "Upload failed.", "error");
    }
}

async function handleConfigCreate(event) {
    event.preventDefault();

    const name = document.getElementById("config-name").value.trim();
    const description = document.getElementById("config-description").value.trim();
    const technique = document.getElementById("privacy-technique").value;
    const quasi = splitCsv(document.getElementById("config-quasi").value);
    const sensitive = splitCsv(document.getElementById("config-sensitive").value);

    if (!quasi.length) {
        showToast("Provide at least one quasi identifier.", "error");
        return;
    }

    const payload = {
        name,
        description,
        quasi_identifiers: quasi,
        sensitive_attributes: sensitive.length ? sensitive : null,
        privacy_technique: technique,
        privacy_parameters: buildTechniqueParameters(technique),
    };

    try {
        const response = await fetchWithAuth("/pipeline/configs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });

        if (!response.ok) {
            const error = await safeJson(response);
            throw new Error(error?.detail || error?.message || "Failed to create configuration");
        }

        showToast("Configuration saved.", "success");
        event.target.reset();
        toggleTechniqueParameters();
        await loadConfigs();
    } catch (err) {
        showToast(err.message || "Unable to save configuration.", "error");
    }
}

async function handleRunExecute(event) {
    event.preventDefault();

    const datasetId = Number(document.getElementById("run-dataset").value);
    const configId = Number(document.getElementById("run-config").value);
    const identifierValue = document.getElementById("run-identifier").value;
    const identifierDatasetId = identifierValue ? Number(identifierValue) : null;

    if (!datasetId || !configId) {
        showToast("Select dataset and configuration.", "error");
        return;
    }

    try {
        const response = await fetchWithAuth("/pipeline/runs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                dataset_id: datasetId,
                config_id: configId,
                identifier_dataset_id: identifierDatasetId,
            }),
        });

        if (!response.ok) {
            const error = await safeJson(response);
            throw new Error(error?.detail || error?.message || "Pipeline run failed");
        }

        const data = await response.json();
        showToast("Pipeline executed successfully.", "success");
        state.selectedRunId = data.id;
        await loadRuns();
        renderRunDetails(data);
    } catch (err) {
        showToast(err.message || "Unable to execute pipeline.", "error");
    }
}

function buildTechniqueParameters(technique) {
    if (technique === "differential_privacy") {
        return {
            epsilon: Number(document.getElementById("param-epsilon").value),
            sensitivity: Number(document.getElementById("param-sensitivity").value),
        };
    }

    return {
        k: Number(document.getElementById("param-k").value),
        suppression_value: document.getElementById("param-suppression").value || "SUPPRESSED",
        generalise_numeric: document.getElementById("param-generalise").checked,
        bin_count: Number(document.getElementById("param-bin-count").value),
    };
}

function splitCsv(value) {
    return value
        .split(",")
        .map((part) => part.trim())
        .filter(Boolean);
}

async function refreshAll() {
    toggleTechniqueParameters();
    await Promise.all([loadDatasets(), loadConfigs(), loadRuns()]);
}

async function loadDatasets() {
    try {
        const response = await fetchWithAuth("/datasets");
        if (!response.ok) {
            throw new Error("Failed to load datasets");
        }
        const datasets = await response.json();
        state.datasets = datasets;
        renderDatasets();
        populateDatasetSelects();
    } catch (err) {
        showToast(err.message || "Unable to fetch datasets.", "error");
    }
}

function renderDatasets() {
    const container = document.getElementById("datasets-table");
    const detailContainer = document.getElementById("dataset-detail");
    if (!container) return;

    if (!state.datasets.length) {
        container.innerHTML = "<p>No datasets uploaded yet.</p>";
        detailContainer.innerHTML = "";
        return;
    }

    const rows = state.datasets
        .map(
            (dataset) => `
            <tr data-id="${dataset.id}">
                <td>${dataset.name}</td>
                <td>${dataset.description || "—"}</td>
                <td>${dataset.row_count ?? "—"}</td>
                <td>${new Date(dataset.uploaded_at).toLocaleString()}</td>
            </tr>
        `
        )
        .join("");

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Description</th>
                    <th>Rows</th>
                    <th>Uploaded</th>
                </tr>
            </thead>
            <tbody>${rows}</tbody>
        </table>
    `;

    container.querySelectorAll("tr[data-id]").forEach((row) => {
        row.addEventListener("click", async () => {
            const id = Number(row.getAttribute("data-id"));
            const dataset = await fetchDatasetDetail(id);
            if (dataset) {
                detailContainer.innerHTML = `
                    <p><strong>Original File:</strong> ${dataset.original_filename}</p>
                    <p><strong>Columns:</strong> ${dataset.column_names?.join(", ") || "—"}</p>
                `;
            }
        });
    });
}

async function fetchDatasetDetail(id) {
    try {
        const response = await fetchWithAuth(`/datasets/${id}`);
        if (!response.ok) {
            throw new Error("Failed to fetch dataset details");
        }
        return await response.json();
    } catch (err) {
        showToast(err.message || "Unable to load dataset details.", "error");
        return null;
    }
}

function populateDatasetSelects() {
    const datasetSelect = document.getElementById("run-dataset");
    const identifierSelect = document.getElementById("run-identifier");
    if (!datasetSelect || !identifierSelect) return;

    datasetSelect.innerHTML = "";
    identifierSelect.innerHTML = `<option value="">None</option>`;

    state.datasets.forEach((dataset) => {
        const option = new Option(dataset.name, dataset.id);
        datasetSelect.appendChild(option.cloneNode(true));

        const identifierOption = new Option(dataset.name, dataset.id);
        identifierSelect.appendChild(identifierOption);
    });
}

async function loadConfigs() {
    try {
        const response = await fetchWithAuth("/pipeline/configs");
        if (!response.ok) {
            throw new Error("Failed to load configurations");
        }
        const configs = await response.json();
        state.configs = configs;
        renderConfigs();
        populateConfigSelect();
    } catch (err) {
        showToast(err.message || "Unable to fetch configurations.", "error");
    }
}

function renderConfigs() {
    const container = document.getElementById("config-list");
    if (!container) return;

    if (!state.configs.length) {
        container.innerHTML = "<p>No configurations created yet.</p>";
        return;
    }

    container.innerHTML = state.configs
        .map(
            (config) => `
            <div class="config-chip">
                <div>
                    <strong>${config.name}</strong>
                    <span>${config.privacy_technique}</span>
                </div>
                <span>${config.quasi_identifiers.join(", ")}</span>
            </div>
        `
        )
        .join("");
}

function populateConfigSelect() {
    const configSelect = document.getElementById("run-config");
    if (!configSelect) return;

    configSelect.innerHTML = "";
    state.configs.forEach((config) => {
        const option = new Option(config.name, config.id);
        configSelect.appendChild(option);
    });
}

async function loadRuns() {
    try {
        const response = await fetchWithAuth("/pipeline/runs");
        if (!response.ok) {
            throw new Error("Failed to load pipeline runs");
        }
        const runs = await response.json();
        state.runs = runs;
        renderRuns();
    } catch (err) {
        showToast(err.message || "Unable to fetch pipeline runs.", "error");
    }
}

function renderRuns() {
    const container = document.getElementById("runs-table");
    if (!container) return;

    if (!state.runs.length) {
        container.innerHTML = "<p>No pipeline runs yet.</p>";
        return;
    }

    const rows = state.runs
        .map(
            (run) => `
            <tr data-id="${run.id}" class="${run.id === state.selectedRunId ? "active-row" : ""}">
                <td>${run.id}</td>
                <td><span class="badge">${run.status}</span></td>
                <td>${new Date(run.started_at).toLocaleString()}</td>
                <td>${run.completed_at ? new Date(run.completed_at).toLocaleString() : "—"}</td>
            </tr>
        `
        )
        .join("");

    container.innerHTML = `
        <table>
            <thead>
                <tr>
                    <th>Run ID</th>
                    <th>Status</th>
                    <th>Started</th>
                    <th>Completed</th>
                </tr>
            </thead>
            <tbody>${rows}</tbody>
        </table>
    `;

    container.querySelectorAll("tr[data-id]").forEach((row) => {
        row.addEventListener("click", async () => {
            const id = Number(row.getAttribute("data-id"));
            const detail = await fetchRunDetail(id);
            if (detail) {
                state.selectedRunId = id;
                renderRuns();
                renderRunDetails(detail);
            }
        });
    });
}

async function fetchRunDetail(id) {
    try {
        const response = await fetchWithAuth(`/pipeline/runs/${id}`);
        if (!response.ok) {
            throw new Error("Failed to fetch run detail");
        }
        return await response.json();
    } catch (err) {
        showToast(err.message || "Unable to load run detail.", "error");
        return null;
    }
}

function renderRunDetails(detail) {
    const container = document.getElementById("run-details");
    if (!container) return;

    const riskMetrics = detail.risk_metrics
        .map(
            (metric) => `
            <div class="metric-card">
                <h3>${metric.name}</h3>
                <div class="value">${Number(metric.value).toFixed(4)}</div>
                <div class="label">${metric.label}</div>
                ${metric.details ? `<small>${JSON.stringify(metric.details)}</small>` : ""}
            </div>
        `
        )
        .join("");

    const utilityMetrics = detail.utility_metrics
        .map(
            (metric) => `
            <div class="metric-card">
                <h3>${metric.name}</h3>
                <div class="value">${Number(metric.value).toFixed(4)}</div>
                <div class="label">${metric.label}</div>
                ${metric.details ? `<small>${JSON.stringify(metric.details)}</small>` : ""}
            </div>
        `
        )
        .join("");

    const downloads = [];
    if (detail.report_html_path || detail.report_pdf_path) {
        downloads.push(`<button class="btn secondary" data-download="report" data-run="${detail.id}">Download Report</button>`);
    }
    if (detail.protected_path) {
        downloads.push(`<button class="btn secondary" data-download="protected" data-run="${detail.id}">Download Protected Dataset</button>`);
    }

    container.innerHTML = `
        <div class="downloads">
            ${downloads.join("")}
        </div>
        <div class="metrics-section">
            <h3>Risk Metrics</h3>
            <div class="chart-container">
                <canvas id="risk-chart"></canvas>
            </div>
            <div class="metrics-grid">${riskMetrics || "<p>No risk metrics recorded.</p>"}</div>
        </div>
        <div class="metrics-section">
            <h3>Utility Metrics</h3>
            <div class="chart-container">
                <canvas id="utility-chart"></canvas>
            </div>
            <div class="metrics-grid">${utilityMetrics || "<p>No utility metrics recorded.</p>"}</div>
        </div>
        <div>
            <h3>Privacy Summary</h3>
            <pre>${JSON.stringify(detail.privacy_summary || {}, null, 2)}</pre>
        </div>
    `;

    // Render charts
    if (detail.risk_metrics && detail.risk_metrics.length > 0) {
        renderMetricChart("risk-chart", detail.risk_metrics, "Risk Assessment");
    }
    if (detail.utility_metrics && detail.utility_metrics.length > 0) {
        renderMetricChart("utility-chart", detail.utility_metrics, "Utility Preservation");
    }

    container.querySelectorAll("[data-download]").forEach((button) => {
        button.addEventListener("click", async () => {
            const runId = button.getAttribute("data-run");
            const kind = button.getAttribute("data-download");
            await downloadArtifact(kind, runId);
        });
    });
}

async function downloadArtifact(kind, runId) {
    const endpoint = kind === "report" ? `/pipeline/runs/${runId}/report` : `/pipeline/runs/${runId}/protected`;
    try {
        const response = await fetchWithAuth(endpoint);
        if (!response.ok) {
            throw new Error("Download failed");
        }
        const blob = await response.blob();
        const filename = getFilenameFromResponse(response) || `${kind}_${runId}`;
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        setTimeout(() => URL.revokeObjectURL(url), 2000);
    } catch (err) {
        showToast(err.message || "Unable to download file.", "error");
    }
}

function getFilenameFromResponse(response) {
    const disposition = response.headers.get("content-disposition");
    if (!disposition) return null;
    const match = disposition.match(/filename="?([^";]+)"?/);
    return match ? match[1] : null;
}

function renderMetricChart(canvasId, metrics, title) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !Chart) return;

    const ctx = canvas.getContext("2d");
    const labels = metrics.map((m) => m.name);
    const values = metrics.map((m) => Number(m.value));

    new Chart(ctx, {
        type: "bar",
        data: {
            labels: labels,
            datasets: [
                {
                    label: title,
                    data: values,
                    backgroundColor: canvasId.includes("risk")
                        ? "rgba(220, 53, 69, 0.6)"
                        : "rgba(40, 167, 69, 0.6)",
                    borderColor: canvasId.includes("risk")
                        ? "rgba(220, 53, 69, 1)"
                        : "rgba(40, 167, 69, 1)",
                    borderWidth: 1,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false,
                },
                title: {
                    display: true,
                    text: title,
                },
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 1,
                },
            },
        },
    });
}

function showToast(message, type = "success") {
    const wrapper = document.getElementById("notifications");
    if (!wrapper) return;

    const toast = document.createElement("div");
    toast.className = `