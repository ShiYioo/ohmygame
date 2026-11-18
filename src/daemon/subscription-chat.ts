import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { streamSimple } from "@earendil-works/pi-ai/api/openai-completions";
import type { ModelRuntime } from "@earendil-works/pi-coding-agent";
import type { CloudSubscriptionSnapshot } from "../shared/cloud-models.js";
import type { CloudModelsClient } from "./cloud-models.js";

export const SUBSCRIPTION_PROVIDER = "ohmygame-subscription";

export async function syncSubscriptionProvider(runtime: ModelRuntime, cloud: CloudModelsClient): Promise<void> {
  registerSubscriptionProvider(runtime, cloud, await cloud.subscription().catch(() => null));
}

export function registerSubscriptionProvider(runtime: ModelRuntime, cloud: CloudModelsClient, snapshot?: CloudSubscriptionSnapshot | null): void {
  const subscription = snapshot?.window_subscription;
  const models = subscription?.active ? subscription.models : [];
  const configured = runtime.getRegisteredProviderConfig(SUBSCRIPTION_PROVIDER);
  if (!models.length) {
    if (configured) runtime.unregisterProvider(SUBSCRIPTION_PROVIDER);
    return;
  }
  const catalog = models.map((model) => {
    // Some relay catalogs omit limits. Keep local compaction and output budgets conservative.
    const { context_window: context, max_output_tokens: output } = model;
    const contextWindow = context && Number.isSafeInteger(context) && context > 0 ? context : 16_384;
    const maxTokens = output && Number.isSafeInteger(output) && output > 0 ? output : 2_048;
    return {
      id: model.id,
      name: `${model.id} · ×${model.multiplier}`,
      input: ["text" as const],
      reasoning: false,
      contextWindow,
      maxTokens: Math.min(maxTokens, contextWindow),
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      compat: { supportsStore: false, supportsDeveloperRole: false, maxTokensField: "max_completion_tokens" as const },
    };
  });
  if (JSON.stringify(configured?.models) === JSON.stringify(catalog)) return;
  runtime.registerProvider(SUBSCRIPTION_PROVIDER, {
    name: "OhMyGame Cloud",
    // Pi requires a configured provider key and URL. The managed handler supplies both at dispatch.
    baseUrl: "https://api.ohmygame.ai/v1",
    apiKey: "ohmygame-managed",
    api: "openai-completions",
    models: catalog,
    streamSimple(model, context, options) {
      const events = createAssistantMessageEventStream();
      void (async () => {
        const relay = await cloud.subscriptionRelay(model.id, options?.signal);
        try {
          let limitMessage: string | undefined;
          const request = streamSimple({ ...model, api: "openai-completions", baseUrl: relay.baseUrl }, context, {
            ...options,
            apiKey: relay.apiKey,
            signal: relay.signal,
            maxRetries: 0,
            fetch: async (input, init) => {
              const response = await (options?.fetch ?? fetch)(input, { ...init, redirect: "error" });
              const limit = response.headers.get("x-subscription-limit");
              if (response.status === 429 && (limit === "five_hour" || limit === "weekly")) {
                const retryAfter = Number(response.headers.get("retry-after"));
                const window = limit === "five_hour" ? "5-hour" : "weekly";
                const reset = retryAfter > 0 ? ` Resets in ${Math.ceil(retryAfter / 60)} min.` : "";
                limitMessage = `Subscription ${window} limit reached.${reset}`;
              }
              return response;
            },
          });
          for await (const event of request) {
            if (event.type === "error" && limitMessage) event.error.errorMessage = limitMessage;
            events.push(event);
          }
          events.end(await request.result());
        } finally {
          relay.finished();
        }
      })().catch(() => {
        const reason = options?.signal?.aborted ? "aborted" : "error";
        const error: AssistantMessage = {
          role: "assistant", api: model.api, provider: model.provider, model: model.id,
          content: [], stopReason: reason, timestamp: Date.now(),
          usage: { input: 0, output: 0, totalTokens: 0, cacheRead: 0, cacheWrite: 0,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
          errorMessage: reason === "aborted" ? "Request cancelled." : "OhMyGame subscription model is unavailable. Check your subscription and try again.",
        };
        events.push({ type: "error", reason, error });
        events.end(error);
      });
      return events;
    },
  });
}
