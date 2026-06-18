# Kubernetes deployment — CRP analyst (API + scalable ML worker)

Two tiers from **one image**: the `crp-api` web tier (small, replica=2) and the
`crp-ml-worker` compute tier (heavy resources, autoscaled by HPA). The API
enqueues pipeline runs to Redis (`PIPELINE_EXECUTOR=worker`); workers consume and
run the ML. Scale the worker without touching the API.

## Files
- `configmap.yaml`   — non-secret env (REDIS_URL, PIPELINE_EXECUTOR=worker, …)
- `secret.example.yaml` — copy to `secret.yaml`, fill DATABASE_URL / SECRET_KEY / OPENAI_API_KEY
- `redis.yaml`       — Redis Deployment + Service (queue + job state)
- `api.yaml`         — API Deployment (uvicorn) + Service
- `ml-worker.yaml`   — ML worker Deployment (`python -m app.worker`, big CPU/RAM)
- `hpa.yaml`         — HorizontalPodAutoscaler for the worker (CPU 70%, 1→6)
- `kustomization.yaml`

## Deploy
```bash
# 1. Build & push the image (from backend/analyst_backend/)
docker build -t YOUR_REGISTRY/crp-analyst:latest .
docker push YOUR_REGISTRY/crp-analyst:latest
#    then set that image in api.yaml + ml-worker.yaml (or `kustomize edit set image`).

# 2. Secrets (don't commit the filled file)
cp secret.example.yaml secret.yaml && $EDITOR secret.yaml
kubectl apply -f secret.yaml

# 3. Apply everything
kubectl apply -k .

# 4. Validate locally first (no cluster needed)
kubectl apply --dry-run=client -k .
```

## Scaling the ML tier
```bash
kubectl scale deploy/crp-ml-worker --replicas=4      # manual, or
kubectl get hpa crp-ml-worker                        # HPA does it on CPU
```
The HPA needs metrics-server installed in the cluster. For queue-depth-based
autoscaling (more precise than CPU), add a KEDA `ScaledObject` on the Redis `ml`
list — left as a follow-up.

## Notes
- **Apply `secret.yaml` first** (step 2) — it is deliberately NOT in
  `kustomization.yaml`, so `kubectl apply -k .` will NOT create it. Both the API
  and the worker read `DATABASE_URL`/`SECRET_KEY` from it; without it they start
  against placeholders and every DB call fails.
- Postgres is assumed external (managed DB). Point `DATABASE_URL` at it. Add a
  Postgres manifest only for throwaway test clusters.
- The image serves on **port 8017** (the Service maps 80 → 8017). Front it with
  an Ingress/LoadBalancer as needed.
- `PIPELINE_EXECUTOR=inprocess` (override in the ConfigMap) reverts the API to
  running pipelines itself — the safety fallback; the worker tier is then idle.
- Worker concurrency = replicas × 1 job each. Raise throughput by raising
  replicas / HPA `maxReplicas`, not threads (ML is CPU-bound).
- Job state lives in Redis and the backend is chosen once at process start. After
  a Redis outage, **restart the API + worker pods** so they re-bind to Redis
  (otherwise they fall back to per-process in-memory state and can't share jobs).
