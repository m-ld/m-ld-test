import { EventEmitter } from 'events';
import LOG from 'loglevel';
import { logTs } from '../util';
import { isRead } from 'json-rql';
import { MeldClone, MeldConfig } from '@m-ld/m-ld-spec';

export type CloneProxy = MeldClone & { close(): Promise<void> };
export type CloneFactory = (config: MeldConfig, tmpDirName: string) => Promise<CloneProxy>;

export interface CloneMessage {
  id: string,
  '@type': string,
  [key: string]: any
}

export interface StartMessage extends CloneMessage {
  '@type': 'start';
  config: MeldConfig & { logLevel: import('loglevel').LogLevelDesc };
  tmpDirName: string;
  requestId: string;
}

export class CloneProcess extends EventEmitter {
  private handler: (msg: CloneMessage) => void;

  constructor(
    private readonly process: typeof global.process,
    cloneFactory: CloneFactory
  ) {
    super();
    process.on('message', (startMsg: StartMessage) => {
      if (startMsg['@type'] === 'start') {
        const { config, tmpDirName, requestId } = startMsg;
        LOG.setLevel(config.logLevel);
        LOG.debug(logTs(), config['@id'], 'config is', JSON.stringify(config));
        cloneFactory(config, tmpDirName).then(meld => {
          this.send(requestId, 'started', { cloneId: config['@id'] });
          this.handler = this.createHandler(meld);
          process.on('message', this.handler);

          meld.follow().subscribe(update =>
            this.send(requestId, 'updated', { body: update }));

          meld.status.subscribe({
            next: status => this.send(requestId, 'status', { body: status }),
            complete: () => this.send(requestId, 'closed'),
            error: err => this.sendError(requestId, err)
          });
        }).catch(err => {
          LOG.error(logTs(), config['@id'], err);
          this.send(requestId, 'unstarted', { err: `${err}` });
        });
      }
    });
    process.send!({ '@type': 'active' });
  }

  createHandler(meld: CloneProxy) {
    return ({ id, '@type': type, request }: CloneMessage) => {
      switch (type) {
        case 'transact':
          return this.transact(meld, id, request);
        case 'stop':
          return this.stop(meld, id);
        case 'destroy':
          return this.destroy(meld, id);
        default:
          this.sendError(id, `No handler for ${type}`);
      }
    };
  }

  transact(meld: MeldClone, requestId: string, request: any) {
    if (isRead(request)) {
      meld.read(request).subscribe({
        next: subject => this.send(requestId, 'next', { body: subject }),
        complete: () => this.send(requestId, 'complete'),
        error: this.errorHandler(requestId)
      });
    } else {
      this.settle(requestId, meld.write(request), 'complete');
    }
  }

  stop(meld: CloneProxy, requestId: string) {
    this.settle(requestId, meld.close(), 'stopped', true);
  }

  destroy(meld: CloneProxy, requestId: string) {
    this.settle(requestId, meld.close(), 'destroyed', true);
  }

  settle(requestId: string, work: Promise<unknown>, resMsgType: string, terminal = false) {
    work.then(() => this.send(requestId, resMsgType))
      .catch(this.errorHandler(requestId))
      .then(() => !terminal || this.process.off('message', this.handler));
  }

  send(requestId: string, type: string, params = {}) {
    this.process.send!({ requestId, '@type': type, ...params },
      (err: any) => err && LOG.warn(logTs(), 'Clone orphaned from orchestrator', err));
  }

  errorHandler(messageId: string) {
    return (err: any) => this.sendError(messageId, err);
  }

  sendError(requestId: string, err: any) {
    LOG.error(logTs(), err);
    return this.send(requestId, 'error', { err: `${err}` });
  }
}