import fetch, { RequestInit, Response } from 'node-fetch';
import EventEmitter from 'events';
import { Transform } from 'stream';
import { MeldConfig } from '@m-ld/m-ld-spec';
import { Pattern, Subject } from 'json-rql';
import { MeldUpdate } from '@m-ld/m-ld-spec/types';

const ids = new Set<string>();
function newId() {
  let id;
  // noinspection StatementWithEmptyBodyJS
  while (ids.has(id = Math.floor(Math.random() * 0xFFFFFFFF).toString(16))) ;
  ids.add(id);
  return id;
}
/**
 * A clone object wraps the orchestrator API for a single clone.
 * @todo this should implement MeldClone
 */
export class Clone extends EventEmitter {
  /**
   * Can be used to set an ambient 'current' domain, for example during a test.
   * This will be used when the clone is started, unless a domain is specified
   * in the passed configuration.
   */
  static domain: string;

  /**
   * The base URL of the orchestrator. Must be set prior to any tests.
   */
  static orchestratorUrl: string;

  constructor(
    private readonly config: Partial<MeldConfig> = {},
    readonly id = newId()
  ) {
    super();
  }

  /**
   * Creates the requested number of clones, with the given configuration. The
   * returned clones are not started.
   * @param count number of clones
   * @param [config] configuration for all clones
   */
  static create(count: number, config?: MeldConfig) {
    return Array(count).fill(undefined).map(() => new Clone(config));
  }

  /**
   * Creates and starts the requested number of clones, with the given
   * configuration. If you want to start a subset of the clones, call
   * {@link create} instead and start them yourself.
   * @param count number of clones
   * @param [config] configuration for all clones
   */
  static async start(count: number, config?: MeldConfig) {
    const clones = Clone.create(count, config);
    await clones[0].start(true);
    await Promise.all(clones.slice(1).map(clone => clone.start()));
    return clones;
  }

  /**
   * Starts a clone. The domain is inferred from the running test name.
   * The 'start' end-point sets up an HTTP Stream. The first chunk is the 'started' message;
   * all subsequent chunks are 'updated' messages, which are emitted by this class as events.
   * http://orchestrator:port/start?cloneId=hexclonid&domain=full-test-name.m-ld.org
   * <= config: { constraints: [{ '@type'... }] }
   * => { '@type': 'started' },
   *    { '@type: 'status', body: MeldStatus },
   *    { '@type: 'updated', body: MeldUpdate }...
   * @param requireOnline if `true`, wait for the clone to have status online
   */
  async start(requireOnline = false) {
    const events: Transform = await send('start', {
      cloneId: this.id,
      domain: this.config?.['@domain'] || Clone.domain
    }, this.config);
    return new Promise((resolve, reject) => {
      events.on('data', event => {
        if (requireOnline && event['@type'] === 'status' && event.body.online)
          resolve(event);
        else if (!requireOnline && event['@type'] === 'started')
          resolve(event);
        this.emit(event['@type'], event.body);
      });
      events.on('end', () => this.emit('closed'));
      events.on('error', reject);
    });
  }

  /**
   * Stops the given clone normally, keeping any persisted data for that ID.
   * http://orchestrator:port/stop?cloneId=hexclonid
   * => { '@type': 'stopped' }
   */
  async stop() {
    return send('stop', { cloneId: this.id });
  };

  /**
   * Kills the clone process completely without any normal shutdown.
   * http://orchestrator:port/kill?cloneId=hexclonid
   * => { '@type': 'killed' }
   */
  async kill() {
    return send('kill', { cloneId: this.id });
  };

  /**
   * Stops the given clone normally and then deletes any persisted data,
   * such that a re-start of the same clone ID will be as if brand-new.
   * Note that this should not error if the clone is already dead, but
   * still destroy its data.
   * http://orchestrator:port/destroy?cloneId=hexclonid
   * => { '@type': 'destroyed' }
   */
  async destroy() {
    return send('destroy', { cloneId: this.id });
  };

  /**
   * Sends the given transaction request to the given clone.
   * http://orchestrator:port/transact?cloneId=hexclonid
   * <= json-rql
   * => Subject ...
   */
  async transact(pattern: Pattern) {
    const subjects: Transform
      = await send('transact', { cloneId: this.id }, pattern);
    // TODO: option to just return the stream
    return new Promise((resolve, reject) => {
      const all: Subject[] = [];
      subjects.on('data', subject => all.push(subject));
      subjects.on('end', () => resolve(all));
      subjects.on('error', reject);
    });
  };

  /**
   * Isolates the clone from the messaging layer.
   * http://orchestrator:port/kill?cloneId=hexclonid
   * => { '@type': 'partitioned' }
   */
  async partition(state = true) {
    return send('partition', { cloneId: this.id, state });
  };

  /**
   * Utility returning a promise that resolves when an update is emitted with
   * the given path. The path matching requires the last path element to be a
   * deep value which has the prior path elements appearing, in order, in its
   * deep path.
   * @param path any sparse path that the update must contain
   * @returns the update
   */
  async updated(...path: any[]): Promise<MeldUpdate> {
    return new Promise(resolve => this.on('updated',
      update => hasPath(update, path) && resolve(update)));
  }

  /**
   * Utility returning a promise that resolves when the given status value has
   * been emitted by the clone.
   */
  async status(status: 'online' | 'outdated' | 'silo', value: boolean): Promise<void> {
    return new Promise(resolve => this.on('status', newStatus => {
      if (newStatus != null && newStatus[status] === value)
        resolve();
    }));
  }

  /**
   * Utility returning a promise when the clone closes. This may be independent of
   * any active stop(), kill() or destroy().
   */
  async closed() {
    return new Promise(resolve => this.on('closed', resolve));
  }
}

async function send(message: string, params: Record<string, any>, body?: any) {
  // noinspection JSUnresolvedReference
  const url = new URL(message, Clone.orchestratorUrl);
  Object.entries(params).forEach(([name, value]) =>
    url.searchParams.append(name, value));
  const options: RequestInit = { method: 'post' };
  if (body) {
    options.body = JSON.stringify(body);
    options.headers = { 'Content-Type': 'application/json' };
  }
  const res = await checkStatus(await fetch(url.toString(), options), url);
  if (res.headers.get('transfer-encoding') === 'chunked') {
    // noinspection JSUnusedGlobalSymbols
    return res.body.pipe(new Transform({
      objectMode: true,
      transform(chunk, _, callback) {
        try {
          callback(null, JSON.parse(chunk.toString()));
        } catch (err) {
          callback(err);
        }
      }
    }));
  } else {
    return res.json();
  }
}

async function checkStatus(res: Response, url: URL) {
  if (res.ok) { // res.status >= 200 && res.status < 300
    return res;
  } else {
    // Try to get the error message from the body
    throw await res.json()
      .then(err => new Error(err.message))
      .catch(() => res.text())
      .catch(() => `message unavailable, ${res.statusText} for ${url}`);
  }
}

function hasPath(obj: any, path: any[]): boolean {
  if (path == null || !path.length || (path.length === 1 && obj === path[0])) {
    return true;
  } else if (typeof path[0] === 'object') {
    return Object.entries(path[0]).every(e => hasPath(obj, e.concat(path.slice(1))));
  } else if (typeof obj === 'object') {
    if (path.length > 1 && path[0] in obj)
      return hasPath(obj[path[0]], path.slice(1));
    else
      return Object.values(obj).some(val => hasPath(val, path));
  }
  return false;
}