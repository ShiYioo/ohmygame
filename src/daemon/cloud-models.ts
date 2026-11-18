import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { BUILTIN_CLOUD_CATALOG, type Cloud3DRequest, type CloudCatalog, type CloudConnectionState, type CloudGenerationJob, type CloudQuotaSnapshot, type CloudSubscriptionSnapshot } from "../shared/cloud-models.js";
import type { MediaModelCatalog, Model3DModel, ProviderSummary } from "../shared/contracts.js";
import { Model3DGenerationError, readModel3DResult, type Model3DGenerationInput, type Model3DGenerator } from "./model3d.js";

/** Product access tokens stay in daemon memory and only go to the configured cloud API. */
export class CloudModelsClient implements Model3DGenerator {
  private session?: { token: string; userId: string; controller: AbortController };
  private metadata: CloudCatalog = BUILTIN_CLOUD_CATALOG;
  private snapshot?: CloudQuotaSnapshot;
  private subscriptionSnapshot?: { token: string; value: CloudSubscriptionSnapshot; until: number };
  private subscriptionPending?: { token: string; request: Promise<CloudSubscriptionSnapshot | null> };
  private subscriptionCatalogRetryAt = 0;
  private chatCredential?: {
    token: string;
    until: number;
    request: Promise<{ baseUrl: string; apiKey: string }>;
  };
  private error?: string;
  private fetchedAt = 0;
  private quotaRevision = 0;
  private refreshPending?: Promise<void>;
  private readonly apiUrl: string;
  constructor(apiUrl: string, private readonly request: typeof fetch = fetch,
    private readonly wait: (milliseconds: number, signal: AbortSignal) => Promise<unknown> = (milliseconds, signal) => delay(milliseconds, undefined, { signal })) {
    const url = new URL(apiUrl);
    if ((url.protocol !== "https:" && !(url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))) || url.username || url.password) throw new Error("Cloud API must use HTTPS (or local development HTTP)");
    this.apiUrl = url.href.replace(/\/$/, "");
  }
  async setSession(input: { accessToken: string; userId: string } | null): Promise<void> {
    if (input && this.session?.userId === input.userId) {
      // An identical auth heartbeat can reuse a recently verified allowance.
      if (this.session.token === input.accessToken) return this.refresh();
      this.session.token = input.accessToken;
      await this.refreshPending;
      return this.refresh(true);
    }
    this.session?.controller.abort();
    this.session = input ? { token: input.accessToken, userId: input.userId, controller: new AbortController() } : undefined;
    this.refreshPending = undefined;
    this.snapshot = undefined;
    this.subscriptionSnapshot = undefined;
    this.subscriptionPending = undefined;
    this.subscriptionCatalogRetryAt = 0;
    this.chatCredential = undefined;
    this.error = undefined;
    if (input) await this.refresh(true);
  }
  async refresh(force = false): Promise<void> {
    const session = this.session;
    if (!session) return;
    if (!force && this.snapshot && Date.now() - this.fetchedAt < 15_000) return;
    if (this.refreshPending) return this.refreshPending;
    const pending = (async () => {
      const token = session.token;
      const revision = this.quotaRevision;
      try {
        const [metadata, snapshot] = await Promise.all([
          this.json<CloudCatalog>("/v1/cloud/catalog", session),
          this.json<CloudQuotaSnapshot>("/v1/me/quotas", session),
        ]);
        if (this.session !== session || session.token !== token || this.quotaRevision !== revision) return;
        if (snapshot.userId !== session.userId || !Array.isArray(snapshot.quotas) || !Array.isArray(metadata.providers) || !Array.isArray(metadata.models)) throw new Model3DGenerationError("Could not verify the cloud account", 401);
        this.metadata = metadata;
        this.snapshot = snapshot;
        this.fetchedAt = Date.now();
        this.error = undefined;
      } catch (cause) {
        if (this.session !== session || session.token !== token || this.quotaRevision !== revision) return;
        this.error = cause instanceof Model3DGenerationError ? cause.message : "Free cloud models are temporarily unavailable";
        if (cause instanceof Model3DGenerationError && cause.statusCode === 401) { await this.setSession(null); }
      }
    })();
    this.refreshPending = pending;
    try { await pending; } finally { if (this.refreshPending === pending) this.refreshPending = undefined; }
  }
  async quotas(): Promise<CloudQuotaSnapshot | null> { await this.refresh(true); return this.snapshot ?? null; }
  subscriptionCatalog() {
    const session = this.session;
    if (!session) return { loading: false };
    const now = Date.now();
    const cached = this.subscriptionSnapshot?.token === session.token ? this.subscriptionSnapshot : undefined;
    const expires = cached?.value.window_subscription.expires_at;
    const snapshot = expires && expires * 1_000 <= now ? undefined : cached?.value;
    if (cached && cached.until > now) return { snapshot, loading: false };
    if (this.subscriptionPending?.token === session.token) return { snapshot, loading: true };
    if (this.subscriptionCatalogRetryAt > now) return { snapshot, loading: false };
    const token = session.token;
    void this.subscription().catch(() => {
      if (this.session === session && session.token === token) this.subscriptionCatalogRetryAt = Date.now() + 10_000;
    });
    return { snapshot, loading: true };
  }
  async subscription(force = false, userId?: string): Promise<CloudSubscriptionSnapshot | null> {
    const session = this.session;
    if (!session || (userId !== undefined && session.userId !== userId)) return null;
    const token = session.token;
    if (this.subscriptionPending?.token === token) return this.subscriptionPending.request;
    const cached = this.subscriptionSnapshot;
    if (!force && cached?.token === token && Date.now() < cached.until) return cached.value;
    const request = (async () => {
      const { data } = await this.json<{ data: CloudSubscriptionSnapshot }>("/api/billing/subscription", session, {
        signal: AbortSignal.timeout(15_000),
      });
      if (this.session !== session || session.token !== token) return null;
      if (!Array.isArray(data?.window_subscription?.models)) {
        throw new Model3DGenerationError("Invalid Cloud subscription response", 502);
      }
      const { expires_at, five_hour, weekly } = data.window_subscription;
      const until = Math.min(Date.now() + 10_000, ...[expires_at, five_hour?.resets_at, weekly?.resets_at]
        .filter((time): time is number => typeof time === "number" && time > 0)
        .map((time) => time * 1_000));
      this.subscriptionSnapshot = { token, value: data, until };
      this.subscriptionCatalogRetryAt = 0;
      return data;
    })();
    this.subscriptionPending = { token, request };
    try {
      return await request;
    } finally {
      if (this.subscriptionPending?.request === request) this.subscriptionPending = undefined;
    }
  }
  async subscriptionRelay(model: string, signal?: AbortSignal) {
    const session = this.session;
    if (!session) throw new Error("Sign in to use subscription models.");
    const subscription = (await this.subscription(false, session.userId))?.window_subscription;
    if (this.session !== session || !subscription?.active || !subscription.models.some(({ id }) => id === model)) {
      throw new Error("This model is not available with the current subscription.");
    }
    const token = session.token;
    let credential = this.chatCredential;
    if (credential?.token !== token || credential.until <= Date.now()) {
      const request = this.json<{ data: { base_url: string; api_key: string } }>("/api/credential", session, {
        method: "POST",
        signal: AbortSignal.timeout(15_000),
      }).then(({ data }) => {
        const url = new URL(data.base_url);
        const local = ["localhost", "127.0.0.1", "[::1]"];
        const localRelay = url.protocol === "http:" && local.includes(url.hostname) && local.includes(new URL(this.apiUrl).hostname);
        if ((!localRelay && url.protocol !== "https:") || url.username || url.password || url.search || url.hash || !data.api_key) {
          throw new Error("Cloud returned an invalid model relay credential.");
        }
        return { baseUrl: url.href.replace(/\/$/, ""), apiKey: data.api_key };
      }).catch((cause) => {
        if (this.chatCredential?.request === request) this.chatCredential = undefined;
        throw cause;
      });
      credential = { token, request, until: Date.now() + 60_000 };
      this.chatCredential = credential;
    }
    const result = await credential.request;
    const combined = signal ? AbortSignal.any([signal, session.controller.signal]) : session.controller.signal;
    combined.throwIfAborted();
    return { ...result, signal: combined, finished: () => {
      if (this.session === session) this.subscriptionSnapshot = undefined;
    } };
  }
  hasProvider(providerId: string): boolean { return this.metadata.providers.some((provider) => provider.id === providerId); }
  /**
   * Return the currently known provider state without starting or awaiting a
   * network request. Settings pages use this snapshot so a slow cloud service
   * cannot hold the whole provider list hostage.
   */
  providersSnapshot(isEnabled: (id: string) => boolean): ProviderSummary[] {
    const available = this.availableProviderIds();
    return this.metadata.providers.map((provider) => {
      const cloud = this.connection(provider.id);
      const configured = available.has(provider.id);
      return {
        id: provider.id,
        name: `${provider.name} · Free`,
        managed: "free_cloud",
        configured,
        cloud,
        enabled: isEnabled(provider.id),
        status: this.error ? "error" : configured ? "connected" : "not_configured",
        capabilities: provider.capabilities,
        methods: [],
      };
    });
  }
  connection(provider: string): CloudConnectionState {
    const quota = this.snapshot?.quotas.find((quota) => quota.provider === provider);
    return { availability: !this.session ? "sign_in_required" : this.error || !quota ? "unavailable" : quota.availability,
      ...(this.session ? { userId: this.session.userId } : {}),
      ...(quota ? { quota } : {}), ...(this.error ? { message: this.error } : quota?.message ? { message: quota.message } : {}) };
  }
  /** Like providersSnapshot, but for the cloud model settings panel. */
  catalogSnapshot(): CloudCatalog {
    const available = this.availableProviderIds();
    const verificationPending = Boolean(this.session && !this.snapshot && !this.error);
    return {
      // Keep providers visible while quota verification is pending. The model
      // settings route uses this list to validate provider ids. Once the
      // allowance is known, hide models that the account cannot use.
      providers: this.metadata.providers,
      models: verificationPending ? this.metadata.models : this.metadata.models.filter((model) => available.has(model.provider)),
    };
  }
  private availableProviderIds(): Set<string> {
    return new Set(this.snapshot?.quotas
      .filter((quota) => quota.availability !== "unavailable")
      .map((quota) => quota.provider) ?? []);
  }
  async catalog(): Promise<MediaModelCatalog<Model3DModel>> {
    await this.refresh();
    const providers = this.metadata.providers.filter((provider) => provider.capabilities.includes("3d")).map((provider) => {
      const cloud = this.connection(provider.id);
      return { provider: provider.id, providerName: `${provider.name} · Free`, cloud, state: cloud.availability === "ready" ? "ready" as const : "empty" as const, message: cloudMessage(cloud) };
    });
    const models = this.metadata.models.filter((model) => model.capability === "3d" && model.polycount && model.maxReferenceImages
      && this.snapshot?.quotas.some((quota) => quota.provider === model.provider && quota.availability !== "unavailable")).map((model) => ({
      ...model, providerName: `${this.metadata.providers.find((provider) => provider.id === model.provider)?.name ?? model.provider} · Free`,
      maxReferenceImages: model.maxReferenceImages!, polycount: model.polycount!,
    }));
    return { models, providers };
  }
  async jobs(): Promise<CloudGenerationJob[]> { return this.json("/v1/cloud/jobs", this.requireSession()); }
  async result(id: string, signal?: AbortSignal): Promise<Buffer> {
    const response = await this.response(`/v1/cloud/jobs/${encodeURIComponent(id)}/result`, this.requireSession(), { signal });
    return readModel3DResult(response);
  }
  async generate(input: Model3DGenerationInput, signal?: AbortSignal) {
    const session = this.requireSession();
    const combined = AbortSignal.any([session.controller.signal, ...(signal ? [signal] : []), AbortSignal.timeout(25 * 60_000)]);
    const body: Cloud3DRequest = { provider: input.model.provider, modelId: input.model.id,
      images: input.images.map((image) => ({ mediaType: image.mediaType as "image/png" | "image/jpeg", data: image.data })),
      targetPolycount: input.targetPolycount, texture: input.texture, pbr: input.pbr };
    const requestKey = randomUUID();
    const create = () => this.json<CloudGenerationJob>("/v1/cloud/3d/jobs", session, { method: "POST", signal: combined,
      headers: { "content-type": "application/json", "idempotency-key": requestKey }, body: JSON.stringify(body) });
    try {
      let job: CloudGenerationJob;
      try { job = await create(); }
      catch (cause) {
        // Retry only transport/server failures at our idempotent endpoint, using the same key.
        if (combined.aborted || cause instanceof Model3DGenerationError && cause.statusCode < 500) throw cause;
        job = await create();
      }
      this.invalidateQuotas(session);
      while (job.status === "queued" || job.status === "running") {
        const pollAfter = Number.isFinite(job.pollAfterMs) ? job.pollAfterMs : 5_000;
        await this.wait(Math.max(5_000, Math.min(20 * 60_000, pollAfter)), combined);
        job = await this.json<CloudGenerationJob>(`/v1/cloud/jobs/${encodeURIComponent(job.id)}`, session, { signal: combined });
      }
      if (job.status !== "succeeded") throw new Model3DGenerationError(job.message ?? "Cloud generation could not complete. Its job is retained on your account", 502);
      const response = await this.response(`/v1/cloud/jobs/${encodeURIComponent(job.id)}/result`, session, { signal: combined });
      return { bytes: await readModel3DResult(response), mediaType: "model/gltf-binary" as const, requestId: job.id };
    } finally {
      // Even a lost submission response may have reserved quota on the server.
      this.invalidateQuotas(session);
      // Cancelling desktop waiting does not cancel or refund an accepted cloud job.
    }
  }
  private invalidateQuotas(session: NonNullable<CloudModelsClient["session"]>) {
    if (this.session !== session) return;
    this.quotaRevision++;
    this.snapshot = undefined;
  }
  private requireSession() {
    if (!this.session) throw new Model3DGenerationError("Sign in to use free cloud models", 401);
    return this.session;
  }
  private async response(path: string, session: NonNullable<CloudModelsClient["session"]>, init: RequestInit = {}) {
    const response = await this.request(`${this.apiUrl}${path}`, { ...init, redirect: "error",
      signal: AbortSignal.any([session.controller.signal, AbortSignal.timeout(init.method === "POST" ? 90_000 : 70_000), ...(init.signal ? [init.signal] : [])]),
      headers: { ...init.headers, authorization: `Bearer ${session.token}` } });
    if (!response.ok) {
      const body = await response.json().catch(() => ({})) as { message?: unknown };
      throw new Model3DGenerationError(typeof body.message === "string" ? body.message : "Free cloud request failed", response.status);
    }
    return response;
  }
  private async json<T>(path: string, session: NonNullable<CloudModelsClient["session"]>, init: RequestInit = {}): Promise<T> { return (await this.response(path, session, init)).json() as Promise<T>; }
}
export function cloudMessage(cloud: CloudConnectionState): string {
  if (cloud.message) return cloud.message;
  return { ready: "Free cloud generation", sign_in_required: "Sign in to use free cloud models", unavailable: "Free cloud generation is temporarily unavailable",
    personal_exhausted: "Your daily free quota is exhausted" }[cloud.availability];
}
