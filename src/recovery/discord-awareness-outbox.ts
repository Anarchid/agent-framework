import {
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import { randomUUID } from 'node:crypto';

export const DEFAULT_DISCORD_AWARENESS_EMOJI = '💤';

export function defaultDiscordAwarenessOutboxPath(storePath: string): string {
  return join(storePath, 'recovery', 'discord-awareness-outbox.json');
}

export interface DiscordAwarenessRef {
  serverId: string;
  channelId: string;
  messageId: string;
}

export type DiscordAwarenessAction = 'add' | 'remove';
export type DiscordAwarenessDeliveryStatus = 'pending' | 'applied' | 'permanent-failure';

export interface DiscordAwarenessEntry extends DiscordAwarenessRef {
  desired: boolean;
  markerPresent: boolean;
  deliveryStatus: DiscordAwarenessDeliveryStatus;
  attempts: number;
  lastAction?: DiscordAwarenessAction;
  lastAttemptAt?: number;
  lastError?: string;
}

export interface DiscordAwarenessBatch {
  id: string;
  status: 'prepared' | 'active';
  agentName: string;
  sourceBranch: string;
  targetBranch: string;
  emoji: string;
  createdAt: number;
  refs: DiscordAwarenessEntry[];
  /**
   * `target-branch` may be promoted when its target branch (or a descendant)
   * is active. `explicit` requires activate() after a second mutation.
   */
  activationPolicy?: 'target-branch' | 'explicit';
  /** Idempotent interval operations used to resume an interrupted suppression. */
  suppressionIntervals?: DiscordSuppressionInterval[];
}

export interface DiscordSuppressionInterval {
  fromId: string;
  toId: string;
}

export interface DiscordAwarenessOperation {
  batchId: string;
  emoji: string;
  action: DiscordAwarenessAction;
  ref: DiscordAwarenessEntry;
}

interface DiscordAwarenessOutboxDocument {
  version: 2;
  batches: DiscordAwarenessBatch[];
}

interface BranchDescriptor {
  id: string;
  name: string;
  parentId?: string;
}

export interface DiscordMessageMetadataCarrier {
  metadata?: Record<string, unknown>;
}

/** Extract Discord addressing metadata without reading message content. */
export function extractDiscordAwarenessRefs(
  messages: DiscordMessageMetadataCarrier[],
  forcedServerId?: string,
): DiscordAwarenessRef[] {
  const refs = new Map<string, DiscordAwarenessRef>();

  for (const message of messages) {
    const metadata = message.metadata ?? {};
    const channelId = typeof metadata.channelId === 'string' ? metadata.channelId : '';
    const messageId = typeof metadata.messageId === 'string' ? metadata.messageId : '';
    const metadataServerId = typeof metadata.serverId === 'string' ? metadata.serverId : '';
    const serverId = forcedServerId ?? metadataServerId;
    if (!channelId || !messageId || !serverId) continue;

    const isDiscord = serverId.toLowerCase().includes('discord') || channelId.startsWith('discord:');
    if (!isDiscord) continue;

    const ref = { serverId, channelId, messageId };
    refs.set(refKey(ref), ref);
  }

  return [...refs.values()];
}

/**
 * A branch-independent, durable operation ledger for Discord awareness marks.
 *
 * Completed entries are retained. Reconciliation derives whether their emoji
 * should currently exist from Chronicle branch ancestry, queuing `remove`
 * after switching away from a recovery branch and `add` when returning to it.
 */
export class DiscordAwarenessOutbox {
  constructor(readonly path: string) {}

  prepare(input: {
    agentName: string;
    sourceBranch: string;
    targetBranch: string;
    refs: DiscordAwarenessRef[];
    emoji?: string;
    activationPolicy?: 'target-branch' | 'explicit';
    suppressionIntervals?: DiscordSuppressionInterval[];
  }): DiscordAwarenessBatch | null {
    const refs = dedupeRefs(input.refs);
    if (refs.length === 0 && !input.suppressionIntervals?.length) return null;

    const document = this.read();
    const batch: DiscordAwarenessBatch = {
      id: randomUUID(),
      status: 'prepared',
      agentName: input.agentName,
      sourceBranch: input.sourceBranch,
      targetBranch: input.targetBranch,
      emoji: input.emoji ?? DEFAULT_DISCORD_AWARENESS_EMOJI,
      createdAt: Date.now(),
      refs: refs.map((ref) => ({
        ...ref,
        desired: false,
        markerPresent: false,
        deliveryStatus: 'applied',
        attempts: 0,
      })),
      activationPolicy: input.activationPolicy ?? 'target-branch',
      suppressionIntervals: input.suppressionIntervals?.map((interval) => ({ ...interval })),
    };
    document.batches.push(batch);
    this.write(document);
    return structuredClone(batch);
  }

  /** Activate after Chronicle has safely switched to the target branch. */
  activate(batchId: string): void {
    const document = this.read();
    const batch = document.batches.find((candidate) => candidate.id === batchId);
    if (!batch) throw new Error(`Discord awareness outbox batch not found: ${batchId}`);
    batch.status = 'active';
    for (const ref of batch.refs) setDesired(ref, true);
    this.write(document);
  }

  /**
   * Retire a batch that was prepared but whose operation did not complete
   * (fork failed, a removal threw and the source branch was restored). A
   * prepared batch is armed: it re-fires at boot or on branch switch through
   * `activatePreparedForBranch` / `preparedSuppressionsForBranch`, and an
   * un-completable suppression there aborts framework start. Only prepared
   * batches can be discarded — an active batch owns delivered markers and
   * must be reconciled, not dropped. Returns false when nothing was removed.
   */
  discard(batchId: string): boolean {
    const document = this.read();
    const index = document.batches.findIndex((candidate) => candidate.id === batchId);
    if (index < 0) return false;
    if (document.batches[index].status !== 'prepared') return false;
    document.batches.splice(index, 1);
    this.write(document);
    return true;
  }

  /**
   * Reconcile every retained ledger entry against the active branch. A marker
   * is desired when any active batch for the same ref+emoji has a target branch
   * in the active branch's ancestry.
   */
  reconcileForBranch(
    branchName: string,
    branches: BranchDescriptor[] = [],
  ): { activated: number; queued: number } {
    const document = this.read();
    let activated = 0;

    for (const batch of document.batches) {
      if (
        batch.status === 'prepared'
        && (batch.activationPolicy ?? 'target-branch') === 'target-branch'
        && isTargetActive(branchName, batch.targetBranch, branches)
      ) {
        batch.status = 'active';
        activated++;
      }
    }

    const desiredKeys = new Set<string>();
    for (const batch of document.batches) {
      if (batch.status !== 'active') continue;
      if (!isTargetActive(branchName, batch.targetBranch, branches)) continue;
      for (const ref of batch.refs) desiredKeys.add(markerKey(batch.emoji, ref));
    }

    let queued = 0;
    for (const batch of document.batches) {
      if (batch.status !== 'active') continue;
      for (const ref of batch.refs) {
        setDesired(ref, desiredKeys.has(markerKey(batch.emoji, ref)));
        if (ref.deliveryStatus === 'pending') queued++;
      }
    }

    this.write(document);
    return { activated, queued };
  }

  /** Backward-compatible startup helper. */
  activatePreparedForBranch(branchName: string, branches: BranchDescriptor[] = []): number {
    return this.reconcileForBranch(branchName, branches).activated;
  }

  pending(serverId?: string): DiscordAwarenessOperation[] {
    const operations: DiscordAwarenessOperation[] = [];
    for (const batch of this.read().batches) {
      if (batch.status !== 'active') continue;
      for (const ref of batch.refs) {
        if (ref.deliveryStatus !== 'pending') continue;
        if (serverId !== undefined && ref.serverId !== serverId) continue;
        operations.push({
          batchId: batch.id,
          emoji: batch.emoji,
          action: ref.desired ? 'add' : 'remove',
          ref: { ...ref },
        });
      }
    }
    return operations;
  }

  recordSuccess(batchId: string, ref: DiscordAwarenessRef, action: DiscordAwarenessAction): void {
    const document = this.read();
    const entry = findEntry(document, batchId, ref);
    if (!entry) return;
    entry.markerPresent = action === 'add';
    entry.deliveryStatus = entry.markerPresent === entry.desired ? 'applied' : 'pending';
    entry.attempts++;
    entry.lastAction = action;
    entry.lastAttemptAt = Date.now();
    delete entry.lastError;
    this.write(document);
  }

  recordFailure(
    batchId: string,
    ref: DiscordAwarenessRef,
    action: DiscordAwarenessAction,
    error: string,
    permanent: boolean,
  ): void {
    const document = this.read();
    const entry = findEntry(document, batchId, ref);
    if (!entry) return;
    entry.deliveryStatus = permanent ? 'permanent-failure' : 'pending';
    entry.attempts++;
    entry.lastAction = action;
    entry.lastAttemptAt = Date.now();
    entry.lastError = error;
    this.write(document);
  }

  /** Compatibility alias for older callers that only acknowledged adds. */
  acknowledge(batchId: string, ref: DiscordAwarenessRef): void {
    this.recordSuccess(batchId, ref, 'add');
  }

  batches(): DiscordAwarenessBatch[] {
    return structuredClone(this.read().batches);
  }

  preparedSuppressionsForBranch(
    branchName: string,
    branches: BranchDescriptor[] = [],
  ): DiscordAwarenessBatch[] {
    return structuredClone(this.read().batches.filter((batch) =>
      batch.status === 'prepared'
      && batch.activationPolicy === 'explicit'
      && !!batch.suppressionIntervals?.length
      && isTargetActive(branchName, batch.targetBranch, branches)));
  }

  private read(): DiscordAwarenessOutboxDocument {
    let parsed: unknown;
    try {
      parsed = JSON.parse(readFileSync(this.path, 'utf8'));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        return { version: 2, batches: [] };
      }
      throw new Error(
        `Could not read Discord awareness outbox ${this.path}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }

    if (isDocumentV2(parsed)) return parsed;
    if (isDocumentV1(parsed)) return migrateV1(parsed);
    throw new Error(`Invalid Discord awareness outbox document: ${this.path}`);
  }

  private write(document: DiscordAwarenessOutboxDocument): void {
    mkdirSync(dirname(this.path), { recursive: true });
    const temporary = `${this.path}.${process.pid}.${randomUUID()}.tmp`;
    writeFileSync(temporary, `${JSON.stringify(document, null, 2)}\n`, { mode: 0o600 });
    renameSync(temporary, this.path);
  }
}

function setDesired(ref: DiscordAwarenessEntry, desired: boolean): void {
  const changed = ref.desired !== desired;
  ref.desired = desired;
  if (ref.markerPresent === desired) {
    ref.deliveryStatus = 'applied';
    return;
  }
  const action: DiscordAwarenessAction = desired ? 'add' : 'remove';
  if (changed || ref.lastAction !== action || ref.deliveryStatus !== 'permanent-failure') {
    ref.deliveryStatus = 'pending';
  }
}

function isTargetActive(
  activeName: string,
  targetName: string,
  branches: BranchDescriptor[],
): boolean {
  if (activeName === targetName) return true;
  const byName = new Map(branches.map((branch) => [branch.name, branch]));
  const byId = new Map(branches.map((branch) => [branch.id, branch]));
  let cursor = byName.get(activeName);
  const visited = new Set<string>();
  while (cursor?.parentId && !visited.has(cursor.id)) {
    visited.add(cursor.id);
    cursor = byId.get(cursor.parentId);
    if (cursor?.name === targetName) return true;
  }
  return false;
}

function findEntry(
  document: DiscordAwarenessOutboxDocument,
  batchId: string,
  ref: DiscordAwarenessRef,
): DiscordAwarenessEntry | undefined {
  return document.batches
    .find((batch) => batch.id === batchId)
    ?.refs.find((candidate) => refKey(candidate) === refKey(ref));
}

function dedupeRefs(refs: DiscordAwarenessRef[]): DiscordAwarenessRef[] {
  const deduped = new Map<string, DiscordAwarenessRef>();
  for (const ref of refs) {
    if (!ref.serverId || !ref.channelId || !ref.messageId) continue;
    deduped.set(refKey(ref), { ...ref });
  }
  return [...deduped.values()];
}

function refKey(ref: DiscordAwarenessRef): string {
  return `${ref.serverId}\0${ref.channelId}\0${ref.messageId}`;
}

function markerKey(emoji: string, ref: DiscordAwarenessRef): string {
  return `${emoji}\0${refKey(ref)}`;
}

function isEntry(value: unknown): value is DiscordAwarenessEntry {
  if (!value || typeof value !== 'object') return false;
  const entry = value as Partial<DiscordAwarenessEntry>;
  return typeof entry.serverId === 'string'
    && typeof entry.channelId === 'string'
    && typeof entry.messageId === 'string'
    && typeof entry.desired === 'boolean'
    && typeof entry.markerPresent === 'boolean'
    && (entry.deliveryStatus === 'pending'
      || entry.deliveryStatus === 'applied'
      || entry.deliveryStatus === 'permanent-failure')
    && typeof entry.attempts === 'number'
    && (entry.lastAction === undefined || entry.lastAction === 'add' || entry.lastAction === 'remove')
    && (entry.lastAttemptAt === undefined || typeof entry.lastAttemptAt === 'number')
    && (entry.lastError === undefined || typeof entry.lastError === 'string');
}

function isDocumentV2(value: unknown): value is DiscordAwarenessOutboxDocument {
  if (!value || typeof value !== 'object') return false;
  const document = value as { version?: unknown; batches?: unknown };
  if (document.version !== 2 || !Array.isArray(document.batches)) return false;
  return document.batches.every((batch) => {
    if (!batch || typeof batch !== 'object') return false;
    const candidate = batch as Partial<DiscordAwarenessBatch>;
    return typeof candidate.id === 'string'
      && (candidate.status === 'prepared' || candidate.status === 'active')
      && typeof candidate.agentName === 'string'
      && typeof candidate.sourceBranch === 'string'
      && typeof candidate.targetBranch === 'string'
      && typeof candidate.emoji === 'string'
      && typeof candidate.createdAt === 'number'
      && Array.isArray(candidate.refs)
      && (candidate.activationPolicy === undefined
        || candidate.activationPolicy === 'target-branch'
        || candidate.activationPolicy === 'explicit')
      && (candidate.suppressionIntervals === undefined
        || (Array.isArray(candidate.suppressionIntervals)
          && candidate.suppressionIntervals.every((interval) =>
            !!interval
            && typeof interval.fromId === 'string'
            && typeof interval.toId === 'string')))
      && candidate.refs.every(isEntry);
  });
}

interface LegacyDocument {
  version: 1;
  batches: Array<{
    id: string;
    status: 'prepared' | 'pending';
    agentName: string;
    sourceBranch: string;
    targetBranch: string;
    emoji: string;
    createdAt: number;
    refs: DiscordAwarenessRef[];
    activationPolicy?: 'target-branch' | 'explicit';
  }>;
}

function isDocumentV1(value: unknown): value is LegacyDocument {
  if (!value || typeof value !== 'object') return false;
  const document = value as { version?: unknown; batches?: unknown };
  if (document.version !== 1 || !Array.isArray(document.batches)) return false;
  return document.batches.every((batch) => {
    if (!batch || typeof batch !== 'object') return false;
    const candidate = batch as LegacyDocument['batches'][number];
    return typeof candidate.id === 'string'
      && (candidate.status === 'prepared' || candidate.status === 'pending')
      && typeof candidate.agentName === 'string'
      && typeof candidate.sourceBranch === 'string'
      && typeof candidate.targetBranch === 'string'
      && typeof candidate.emoji === 'string'
      && typeof candidate.createdAt === 'number'
      && Array.isArray(candidate.refs)
      && candidate.refs.every((ref) => !!ref
        && typeof ref.serverId === 'string'
        && typeof ref.channelId === 'string'
        && typeof ref.messageId === 'string');
  });
}

function migrateV1(document: LegacyDocument): DiscordAwarenessOutboxDocument {
  return {
    version: 2,
    batches: document.batches.map((batch) => ({
      ...batch,
      status: batch.status === 'pending' ? 'active' : 'prepared',
      refs: batch.refs.map((ref) => ({
        ...ref,
        desired: batch.status === 'pending',
        markerPresent: false,
        deliveryStatus: batch.status === 'pending' ? 'pending' : 'applied',
        attempts: 0,
      })),
    })),
  };
}
