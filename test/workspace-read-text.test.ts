import test, { type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JsStore } from '@animalabs/chronicle';
import { WorkspaceModule } from '../src/modules/workspace/index.js';

async function setup(t: TestContext, content: string) {
  const root = mkdtempSync(join(tmpdir(), 'workspace-read-text-'));
  const store = JsStore.openOrCreate({ path: join(root, 'store') });
  const workspace = new WorkspaceModule({
    mounts: [{ name: 'work', path: root, mode: 'read-write', watch: 'never' }],
  });
  workspace.initStore(store);
  t.after(() => { store.close(); rmSync(root, { recursive: true, force: true }); });
  assert.equal((await workspace.writeBinary('work/text.txt', Buffer.from(content), 'text/plain')).success, true);
  const read = (input: Record<string, unknown> = {}) => workspace.handleToolCall({
    id: 'read', name: 'read', input: { path: 'work/text.txt', ...input },
  });
  return { workspace, read };
}

test('line reads retain numbering, ranges, and the 2000-line default', async t => {
  const { read } = await setup(t, Array.from({ length: 2005 }, (_, i) => `line ${i + 1}`).join('\n'));
  const result = await read({ offset: 2, limit: 2 });
  assert.deepEqual(result.data, {
    path: 'work/text.txt', totalLines: 2005, fromLine: 2, toLine: 3,
    note: 'Truncated at 2 lines (file has 2005). Use offset/limit to read more.',
    content: '     2\tline 2\n     3\tline 3',
  });
  const defaults = (await read()).data as { toLine: number; content: string };
  assert.equal(defaults.toLine, 2000);
  assert.equal(defaults.content.split('\n').length, 2000);
});

test('character mode can be selected by either parameter and reports EOF', async t => {
  const { read } = await setup(t, 'a'.repeat(3000));
  assert.deepEqual((await read({ offsetChars: 0 })).data, {
    path: 'work/text.txt', totalChars: 3000, offsetChars: 0, nextOffsetChars: 2000, content: 'a'.repeat(2000),
  });
  assert.equal(((await read({ limitChars: 3 })).data as { content: string }).content, 'aaa');
  assert.deepEqual((await read({ offsetChars: 2999, limitChars: 10 })).data, {
    path: 'work/text.txt', totalChars: 3000, offsetChars: 2999, nextOffsetChars: null, content: 'a',
  });
  for (const offsetChars of [3000, 4000, Number.MAX_SAFE_INTEGER]) {
    assert.deepEqual((await read({ offsetChars })).data, {
      path: 'work/text.txt', totalChars: 3000, offsetChars: 3000, nextOffsetChars: null, content: '',
    });
  }
});

test('character pages preserve Unicode and raw newlines, even with limitChars 1', async t => {
  const body = '😀a\r\n𐐷e\u0301終';
  const { read } = await setup(t, body);
  let offsetChars = 0;
  let recovered = '';
  do {
    const result = await read({ offsetChars, limitChars: 1 });
    assert.equal(result.success, true);
    const page = result.data as { content: string; nextOffsetChars: number | null };
    assert.ok(page.content.length >= 1 && page.content.length <= 2);
    assert.equal(Buffer.from(page.content).toString(), page.content, 'UTF-8 transport cannot replace half a pair');
    recovered += page.content;
    if (page.nextOffsetChars === null) break;
    assert.ok(page.nextOffsetChars > offsetChars);
    offsetChars = page.nextOffsetChars;
  } while (true);
  assert.equal(recovered, body);
  const invalid = await read({ offsetChars: 1 });
  assert.equal(invalid.success, false);
  assert.match(invalid.error!, /surrogate pair/);
});

test('empty text returns an empty final character page', async t => {
  const { read } = await setup(t, '');
  assert.deepEqual((await read({ offsetChars: 0 })).data, {
    path: 'work/text.txt', totalChars: 0, offsetChars: 0, nextOffsetChars: null, content: '',
  });
});

test('character paging rejects invalid numbers and ambiguous line/character inputs', async t => {
  const { read } = await setup(t, 'hello');
  for (const input of [
    { offsetChars: -1 }, { offsetChars: 0.5 }, { offsetChars: NaN }, { offsetChars: Infinity },
    { offsetChars: Number.MAX_SAFE_INTEGER + 1 }, { offsetChars: '0' }, { offsetChars: null },
    { limitChars: 0 }, { limitChars: -1 }, { limitChars: 0.5 }, { limitChars: NaN },
    { limitChars: Infinity }, { limitChars: Number.MAX_SAFE_INTEGER + 1 },
    { limitChars: '2' }, { limitChars: null },
    { offset: 1, offsetChars: 0 }, { limit: 2, limitChars: 1 },
    { offset: 1, limitChars: 1 }, { limit: 2, offsetChars: 0 },
  ]) {
    const result = await read(input);
    assert.equal(result.success, false, JSON.stringify(input));
    assert.equal(result.isError, true);
  }
});

test('character reads use the same mount containment and missing-file checks', async t => {
  const { read } = await setup(t, 'hello');
  for (const path of ['work/../escape', 'unknown/file', 'work/missing']) {
    const result = await read({ path, offsetChars: 0 });
    assert.equal(result.success, false, path);
    assert.equal(result.isError, true);
  }
});
