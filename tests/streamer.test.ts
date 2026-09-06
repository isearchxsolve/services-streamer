import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { createStreamer, Streamer } from '../src/index';
import * as http from 'http';

describe('Streamer', () => {
  let streamer: Streamer;
  const testPort = 34567;

  beforeAll(async () => {
    streamer = createStreamer({ port: testPort, path: '/test-stream' });
    await streamer.start();
  });

  afterAll(async () => {
    await streamer.stop();
  });

  it('should start and stop correctly', async () => {
    expect(streamer).toBeDefined();
    const sessions = streamer.getSessions();
    expect(sessions).toEqual([]);
  });

  it('should track sessions', async () => {
    const sessions = streamer.getSessions();
    expect(Array.isArray(sessions)).toBe(true);
  });

  it('should broadcast messages', () => {
    const result = streamer.broadcast('test', { data: 'hello' });
    // broadcast doesn't return anything, just verify no error
    expect(true).toBe(true);
  });

  it('should return false for non-existent session', () => {
    const result = streamer.send('non-existent', 'test', { data: 'hello' });
    expect(result).toBe(false);
  });

  it('should return undefined for non-existent session info', () => {
    const session = streamer.getSession('non-existent');
    expect(session).toBeUndefined();
  });
});

describe('createStreamer', () => {
  it('should create a streamer instance', () => {
    const streamer = createStreamer({ port: 34568 });
    expect(streamer).toBeDefined();
    expect(typeof streamer.start).toBe('function');
    expect(typeof streamer.stop).toBe('function');
    expect(typeof streamer.broadcast).toBe('function');
    expect(typeof streamer.send).toBe('function');
    expect(typeof streamer.getSessions).toBe('function');
    expect(typeof streamer.getSession).toBe('function');
  });
});
