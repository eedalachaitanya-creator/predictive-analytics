export const environment = {
  production: false,
  apiUrl: 'http://10.0.0.14:8017/api/v1',
  // apiUrl: 'http://localhost:8000/api/v1',
  appVersion: '4.0.0',
  scoutApiUrl: 'http://10.0.0.14:8017',
  // scoutApiUrl: 'http://localhost:8000',
  // Set to true to use built-in mock data (no backend needed).
  // Set to false when the real backend is running.
  //
  // Must stay FALSE whenever the backend is up. The mock interceptor
  // (mock.interceptor.ts) was written against CLT-001 before the
  // backend existed — it returns the same canned stages / dashboard /
  // churn scores for every client, so a client selector switch (e.g.
  // CLT-001 → CLT-002) in the header has no effect on the data shown.
  // That produces the bug where the Run page says "Client config
  // wired · CLT-001" even when CLT-002 is selected.
  useMocks: false,
};
