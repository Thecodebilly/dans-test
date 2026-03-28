const latestTbody = document.querySelector('#latestTable tbody');
const latestMeta = document.querySelector('#latestMeta');
let historyChart;

async function postJson(url) {
  const resp = await fetch(url, { method: 'POST' });
  return resp.json();
}

async function loadLatestPrices() {
  const resp = await fetch('/api/prices/latest');
  const rows = await resp.json();
  latestTbody.innerHTML = '';

  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.ticker}</td>
      <td>${row.company_name ?? ''}</td>
      <td>${row.sector ?? ''}</td>
      <td>${row.price?.toFixed(2) ?? ''}</td>
      <td>${row.captured_at}</td>
    `;
    latestTbody.appendChild(tr);
  });

  latestMeta.textContent = rows.length
    ? `Loaded ${rows.length} symbols. Latest capture hour: ${rows[0].captured_at}`
    : 'No snapshots yet. Click "Capture hourly snapshot now".';
}

async function loadHistory() {
  const ticker = document.getElementById('ticker').value.trim();
  const hours = document.getElementById('hours').value;
  const resp = await fetch(`/api/prices/history?ticker=${encodeURIComponent(ticker)}&hours=${encodeURIComponent(hours)}`);
  const rows = await resp.json();

  const labels = rows.map((r) => r.captured_at);
  const values = rows.map((r) => r.price);

  const ctx = document.getElementById('historyChart').getContext('2d');
  if (historyChart) {
    historyChart.destroy();
  }

  historyChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: `${ticker.toUpperCase()} hourly close`,
          data: values,
          borderColor: '#38bdf8',
          backgroundColor: 'rgba(56, 189, 248, 0.2)',
          tension: 0.15,
        },
      ],
    },
    options: {
      responsive: true,
      scales: {
        y: {
          beginAtZero: false,
        },
      },
    },
  });
}

document.getElementById('bootstrapBtn').addEventListener('click', async () => {
  const data = await postJson('/api/bootstrap');
  alert(`Loaded ${data.symbols_loaded} S&P 500 symbols.`);
});

document.getElementById('snapshotBtn').addEventListener('click', async () => {
  const data = await postJson('/api/snapshot');
  alert(`Upserted ${data.rows_upserted} price rows for this hour.`);
  await loadLatestPrices();
});

document.getElementById('refreshLatestBtn').addEventListener('click', loadLatestPrices);
document.getElementById('loadHistoryBtn').addEventListener('click', loadHistory);

loadLatestPrices();
loadHistory();
