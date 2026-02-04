let counter = 0;
jest.mock('uuid', () => ({
  v4: () => `test-uuid-${++counter}`,
}));

import { createSession, getSession, updateSession } from '@/lib/session-store';

describe('session-store', () => {
  test('createSession initialises new fields to null/defaults', () => {
    const session = createSession();
    expect(session.sources_list).toBeNull();
    expect(session.schema_left).toBeNull();
    expect(session.schema_right).toBeNull();
    expect(session.scope_left).toBeNull();
    expect(session.scope_right).toBeNull();
    expect(session.validation_approved).toBe(false);
    expect(session.phase).toBe('greeting');
  });

  test('updateSession can set sources_list', () => {
    const session = createSession();
    const sources = [{ alias: 'inv', uri: 'file://inv.csv', source_type: 'csv', status: 'connected' }];
    updateSession(session.id, { sources_list: sources });
    const updated = getSession(session.id)!;
    expect(updated.sources_list).toEqual(sources);
  });

  test('updateSession can set schema_left and schema_right', () => {
    const session = createSession();
    const schema = [{ name: 'id', data_type: 'Int64', nullable: false }];
    updateSession(session.id, { schema_left: schema, schema_right: schema });
    const updated = getSession(session.id)!;
    expect(updated.schema_left).toEqual(schema);
    expect(updated.schema_right).toEqual(schema);
  });

  test('updateSession can set validation_approved', () => {
    const session = createSession();
    updateSession(session.id, { validation_approved: true });
    const updated = getSession(session.id)!;
    expect(updated.validation_approved).toBe(true);
  });
});
