import { EventEmitter } from 'events';
import LOG from 'loglevel';
import { logTs } from '../util';
import { isRead } from 'json-rql';
import { MeldClone, MeldConfig } from '@m-ld/m-ld-spec';

export type CloneFactory = (config: MeldConfig, tmpDirName: string) =>
  Promise<MeldClone & { close(): Promise<void> }>;

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
  constructor(
    private readonly process: typeof global.process,
    cloneFactory: CloneFactory
  ) {
    super();
    process.on('message', (startMsg: StartMessage) => {
      if (startMsg['@type'] !== 'start')
        return;

      const { config, tmpDirName, requestId } = startMsg;
      LOG.setLevel(config.logLevel);
      LOG.debug(logTs(), config['@id'], 'config is', JSON.stringify(config));
      cloneFactory(config, tmpDirName).then(meld => {
        this.send(requestId, 'started', { cloneId: config['@id'] });

        const handler = (message: CloneMessage) => {
          const settle = (work: Promise<unknown>, resMsgType: string, terminal = false) => {
            work.then(() => this.send(message.id, resMsgType))
              .catch(this.errorHandler(message))
              .then(() => !terminal || process.off('message', handler));
          };
          switch (message['@type']) {
            case 'transact':
              if (isRead(message.request))
                meld.read(message.request).subscribe({
                  next: subject => this.send(message.id, 'next', { body: subject }),
                  complete: () => this.send(message.id, 'complete'),
                  error: this.errorHandler(message)
                });
              else
                settle(meld.write(message.request), 'complete');
              break;
            case 'stop':
              settle(meld.close(), 'stopped', true);
              break;
            case 'destroy':
              settle(meld.close(), 'destroyed', true);
              break;
            default:
              this.sendError(message.id, `No handler for ${message['@type']}`);
          }
        };
        process.on('message', handler);

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
    });
    process.send!({ '@type': 'active' });
  }

  send(requestId: string, type: string, params = {}) {
    this.process.send!({ requestId, '@type': type, ...params },
      (err: any) => err && LOG.warn(logTs(), 'Clone orphaned from orchestrator', err));
  }

  errorHandler(message: CloneMessage) {
    return (err: any) => this.sendError(message.id, err);
  }

  sendError(requestId: string, err: any) {
    LOG.error(logTs(), err);
    return this.send(requestId, 'error', { err: `${err}` });
  }
}

module.exports = CloneProcess;