/**
 * kv-unified receipt flight vs. the framework's error-policy retry.
 *
 * Field incident (devops agent on gpt-6-astra, 2026-09-16 10:42 local):
 *   [inference-failed] kv-unified submission devops:46:1789544019150:4 is still in flight
 *
 * Membrane fires the cache-wire receipt once per provider attempt, BEFORE the
 * adapter call, and the framework only settles that flight on the attempt's
 * usage event (accept) or in driveStream's `finally` (fail). A provider call
 * that dies before its usage event therefore leaves the flight open until the
 * stream is torn down — but the error-policy retry starts the successor stream
 * from INSIDE the failed stream's event loop, before that `finally` runs. The
 * successor's first receipt then hits the ledger's single-flight guard and the
 * retry itself fails with "is still in flight", turning one transient provider
 * error into a lost turn.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { NormalizedRequest, StreamEvent, YieldingStream } from '@animalabs/membrane';
import { AutobiographicalStrategy } from '@animalabs/context-manager';
import { AgentFramework } from '../src/index.js';
import type {
  ErrorAction,
  ErrorPolicy,
  EventResponse,
  Module,
  ModuleContext,
  ProcessEvent,
  ProcessState,
  ToolCall,
  ToolDefinition,
  ToolResult,
  TraceEvent,
} from '../src/index.js';
import type { KvUnifiedRequestHooks } from '../src/kv-unified-wire.js';
import { createMockResponse, MockMembrane } from './helpers/mock-membrane.js';

/** Mirrors membrane's yielding stream closely enough for the receipt seam:
 * the wire receipt fires inside streamOnce, after `await applyBeforeRequestHook`,
 * i.e. a microtask or two after the consumer's first pull, and before any
 * usage event. A receipt hook that throws surfaces as an `error` event (the
 * real stream's startInference catch), never as a thrown `next()`. */
class ScriptedStream {
  private done = false;
  constructor(
    private readonly onFirstPull: () => void,
    private readonly script: StreamEvent[],
  ) {}
  cancel(): void { this.done = true; }
  get isWaitingForTools(): boolean { return false; }
  get pendingToolCallIds(): string[] { return []; }
  get toolDepth(): number { return 0; }
  provideToolResults(): void { throw new Error('ScriptedStream is not waiting for tools'); }
  async *[Symbol.asyncIterator](): AsyncIterator<StreamEvent> {
    await Promise.resolve();
    await Promise.resolve();
    try {
      this.onFirstPull();
    } catch (error) {
      yield { type: 'error', error: error as Error } as StreamEvent;
      return;
    }
    for (const event of this.script) {
      if (this.done) return;
      yield event;
    }
  }
}

/** First provider attempt dies after the receipt fired and before any usage
 * event (a transport error); every later attempt completes normally. */
class DeadThenAliveMembrane extends MockMembrane {
  readonly order: string[] = [];
  private attempts = 0;

  streamYielding(request: NormalizedRequest): YieldingStream {
    this.calls.push(request);
    const attempt = this.attempts++;
    const hooks = request as NormalizedRequest & KvUnifiedRequestHooks;
    const fireReceipt = () => {
      this.order.push(`pull:${attempt}`);
      hooks.onCacheWireReceipt?.({ requestHash: `wire-${attempt}`, markers: [] });
      this.order.push(`receipt:${attempt}`);
    };
    const script: StreamEvent[] = attempt === 0
      ? [{ type: 'error', error: new Error('socket hang up') } as StreamEvent]
      : [
          { type: 'usage', usage: { inputTokens: 10, outputTokens: 5 } } as StreamEvent,
          {
            type: 'complete',
            response: createMockResponse([{ type: 'text', text: 'recovered' }]),
          } as StreamEvent,
        ];
    return new ScriptedStream(fireReceipt, script) as unknown as YieldingStream;
  }
}

class WakeModule implements Module {
  readonly name = 'wake';
  async start(_ctx: ModuleContext): Promise<void> {}
  async stop(): Promise<void> {}
  getTools(): ToolDefinition[] { return []; }
  async handleToolCall(_call: ToolCall): Promise<ToolResult> {
    return { success: false, error: 'no tools', isError: true };
  }
  async onProcess(event: ProcessEvent, _state: ProcessState): Promise<EventResponse> {
    if (event.type !== 'external-message') return {};
    return {
      addMessages: [{ participant: 'User', content: [{ type: 'text', text: String(event.content) }] }],
      requestInference: true,
    };
  }
}

class RetryOncePolicy implements ErrorPolicy {
  maxRetries = 1;
  onInferenceError(_error: Error, _agentName: string, attempt: number): ErrorAction {
    return attempt < this.maxRetries ? { retry: true, delayMs: 0 } : { retry: false };
  }
}

function kvUnifiedStrategy(): AutobiographicalStrategy {
  return new AutobiographicalStrategy({
    adaptiveResolution: true,
    foldingStrategy: 'kv-unified',
    headWindowTokens: 0,
    recentWindowTokens: 100,
    kvUnified: {
      policy: {
        alpha: 0.7,
        budgetLowRatio: 0.5,
        budgetHighRatio: 0.9,
        budgetUnderLambda: 10,
        budgetOverLambda: 10,
        cacheLambda: 1,
        cacheScale: 1000,
        cacheReadPrice: 0.1,
        cacheWritePrice: 1.25,
        continuityLambda: 1,
        continuityScale: 1000,
        continuityRecencyHalfLifeTokens: 1000,
        continuityRecencyFloor: 0.2,
        continuityStableHalfLife: 10,
        continuityStableFloor: 0.25,
      },
      tokenBucketSize: 100,
      continuityBucketSize: 100,
      fidelityBucketSize: 100,
      labelCeiling: 10_000,
      adoptEpsilon: 0,
      treeifyNonContiguousSummaries: false,
      preserveGapBearingSummaries: true,
    },
  } as ConstructorParameters<typeof AutobiographicalStrategy>[0]);
}

test('error-policy retry settles the dead attempt\'s kv-unified flight before the successor submits', async () => {
  const tempDir = mkdtempSync(join(tmpdir(), 'kv-open-flight-'));
  const membrane = new DeadThenAliveMembrane();
  const strategy = kvUnifiedStrategy();
  // Observe the ledger from the outside: the framework reaches the strategy
  // through getStrategy(), so instance-level wrapping sees every call.
  const spied = strategy as unknown as {
    reportKvUnifiedFailed: (id: string) => void;
    reportKvUnifiedAccepted: (args: { submissionId: string }) => void;
  };
  const origFail = spied.reportKvUnifiedFailed.bind(strategy);
  spied.reportKvUnifiedFailed = (id: string) => { membrane.order.push(`fail:${id}`); origFail(id); };
  const origAccept = spied.reportKvUnifiedAccepted.bind(strategy);
  spied.reportKvUnifiedAccepted = (args: { submissionId: string }) => {
    membrane.order.push(`accept:${args.submissionId}`);
    origAccept(args);
  };

  const framework = await AgentFramework.create({
    storePath: join(tempDir, 'store.chronicle'),
    membrane: membrane.asMembrane(),
    agents: [{ name: 'devops', model: 'test-model', systemPrompt: 'Test', strategy }],
    modules: [new WakeModule()],
    errorPolicy: new RetryOncePolicy(),
    syncIntervalMs: 0,
  });
  const traces: TraceEvent[] = [];
  framework.onTrace((event) => traces.push(event));

  try {
    framework.pushEvent({ type: 'external-message', source: 'test', content: 'hello', metadata: {} });
    await framework.runUntilIdle();

    const failures = traces
      .filter((t): t is TraceEvent & { error: string } => t.type === 'inference:failed')
      .map((t) => t.error);
    assert.deepEqual(failures, ['socket hang up'], `only the provider error may fail: ${JSON.stringify(membrane.order)}`);
    assert.ok(traces.some((t) => t.type === 'inference:completed'), `retry must complete the turn: ${JSON.stringify(membrane.order)}`);
    assert.ok(!traces.some((t) => t.type === 'inference:exhausted'), 'a single transient provider error must not exhaust the turn');

    // The dead attempt's flight is closed BEFORE the successor's first pull,
    // so the successor's receipt never meets an open flight.
    const failIndex = membrane.order.findIndex((entry) => entry.startsWith('fail:'));
    const successorPull = membrane.order.indexOf('pull:1');
    assert.ok(failIndex >= 0, `dead flight was never failed: ${JSON.stringify(membrane.order)}`);
    assert.ok(failIndex < successorPull, `dead flight failed after the successor started: ${JSON.stringify(membrane.order)}`);
    assert.ok(membrane.order.some((entry) => entry.startsWith('accept:')), 'successor flight must be accepted on its usage event');
  } finally {
    await framework.stop();
    rmSync(tempDir, { recursive: true, force: true });
  }
});
