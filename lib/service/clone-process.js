const { EventEmitter } = require('events');
const LOG = require('loglevel');
const { logTs } = require('../util');
const { isRead } = require('json-rql');

class CloneProcess extends EventEmitter {
  /**
   * @param {node:Process} process
   * @param {m_ld:CloneFactory} cloneFactory
   */
  constructor(process, cloneFactory) {
    super();
    this.process = process;
    process.on('message', startMsg => {
      if (startMsg['@type'] !== 'start')
        return;

      const { config, tmpDirName, requestId } = /**@type {m_ld_test:StartMessage}*/startMsg;
      LOG.setLevel(config.logLevel);
      LOG.debug(logTs(), config['@id'], 'config is', JSON.stringify(config));
      cloneFactory(config, tmpDirName).then(meld => {
        this.send(requestId, 'started', { cloneId: config['@id'] });

        const handler = message => {
          const settle = (work, resMsgType, terminal) => {
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
    process.send({ '@type': 'active' });
  }

  send(requestId, type, params) {
    this.process.send({ requestId, '@type': type, ...params },
      err => err && LOG.warn(logTs(), 'Clone orphaned from orchestrator', err));
  }

  errorHandler(message) {
    return err => this.sendError(message.id, err);
  }

  sendError(requestId, err) {
    LOG.error(logTs(), err);
    return this.send(requestId, 'error', { err: `${err}` });
  }
}

module.exports = CloneProcess;