const Jasmine = require('jasmine');
const Clone = require('../lib/clone');
const { join } = require('path');

/**
 * @param {string[]} args Jasmine process arguments and specs
 * @param {object} [opts]
 * @param {string} [opts.localTestPath]
 */
module.exports = function runner(args, opts) {
  const jasmineConfig = {};
  const jasmineEnv = new Jasmine();
  let specs = [], filter = undefined;
  function specFile(spec) {
    // A cwd-relative path selects from the local tests
    const root = spec.startsWith('./') ?
      join(process.cwd(), opts?.localTestPath) : __dirname;
    // Replace plain digits with globs e.g. "1/1" with "1-*/1-*"
    spec = spec.replace(/(\d)(?=\/|$)/g, n => `${n}-*`);
    // Run only specs, unless already qualified
    if (!spec.endsWith('.js'))
      spec = `${spec}.spec.js`;
    return join(root, spec);
  }
  for (let arg of args) {
    const optionMatch = arg.match(/--([\w-]+)(?:="?([^"]+)"?)?/);
    if (optionMatch != null) {
      switch (optionMatch[1]) {
        case 'reporter':
          const Reporter = require(optionMatch[2]);
          jasmineEnv.addReporter(new Reporter());
          break;
        case 'filter':
          filter = optionMatch[2];
          console.log('Filter', filter);
          break;
        case 'stop-on-failure':
          jasmineConfig.stopOnSpecFailure = (optionMatch[2] === 'true');
          break;
        case 'random':
          jasmineConfig.random = (optionMatch[2] === 'true');
          break;
      }
    } else {
      specs.push(specFile(arg));
    }
  }
  if (!specs.length)
    specs = [specFile('*/*')];
  console.log('Running specs', specs, 'with config', jasmineConfig);

  return orchestratorUrl => {
    Clone.orchestratorUrl = orchestratorUrl;
    jasmineEnv.loadConfig(jasmineConfig);
    // https://github.com/jasmine/jasmine-npm/issues/130#issuecomment-381795866
    global.jasmine.DEFAULT_TIMEOUT_INTERVAL = 30000;
    jasmineEnv.addReporter({
      specStarted: (result) => {
        Clone.domain = encodeURIComponent(
          result.fullName
            .toLowerCase()
            .replace(/\s+/g, '-')
        ) + '.m-ld.org';
      }
    });
    return jasmineEnv.execute(specs, filter);
  };
};