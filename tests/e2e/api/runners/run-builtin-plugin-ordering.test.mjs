// Offline lifecycle tests: fake gateway/Newman processes, no provider calls.
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { constants } from "node:fs";
import { access, mkdir, mkdtemp, readFile, readdir, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { withDisposableGateway } from "./run-builtin-plugin-ordering.mjs";

const execFileAsync = promisify(execFile);
const runnerPath = fileURLToPath(new URL("./run-builtin-plugin-ordering.mjs", import.meta.url));
const testDir = await mkdtemp(path.join(tmpdir(), "plugin-ordering-runner-test-"));
let passed = 0;
/** Count a lifecycle scenario only after all of its assertions finish successfully. */
async function test(name, fn) {
  await fn();
  passed++;
  console.log(`ok - ${name}`);
}
/** Create a gateway stub that records its PID and store path for cleanup assertions. */
async function fakeGateway(mode = "ok") {
  const marker = path.join(testDir, `marker-${passed}.json`);
  const binary = path.join(testDir, `gateway-${passed}`);
  await writeFile(binary, `#!${process.execPath}
const fs = require('node:fs');
const http = require('node:http');
const args = process.argv.slice(2);
const appDir = args[args.indexOf('-app-dir') + 1];
const port = Number(args[args.indexOf('-port') + 1]);
const config = JSON.parse(fs.readFileSync(appDir + '/config.json', 'utf8'));
fs.writeFileSync(${JSON.stringify(marker)}, JSON.stringify({ appDir, pid: process.pid, environmentKeys: Object.keys(process.env) }));
if (${JSON.stringify(mode)} === 'exit') process.exit(2);
http.createServer((req, res) => {
  if (${JSON.stringify(mode)} === 'hang') return;
  if (${JSON.stringify(mode)} === 'unauthorized' || req.headers['x-bifrost-setup-token'] !== config.setup_token) {
    res.writeHead(403); res.end('{}'); return;
  }
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ plugins: ${JSON.stringify(mode)} === 'dirty' ? [{ name: 'telemetry' }] : ${JSON.stringify(mode)} === 'resolver-dirty' ? [{ name: 'model-catalog-resolver' }] : [] }));
}).listen(port, '127.0.0.1');
`, { mode: 0o700 });
  return { binary, marker };
}
/** Verify both the fixture process and its temporary store are gone after a run. */
async function assertDiscarded(marker) {
  const { appDir, pid } = JSON.parse(await readFile(marker, "utf8"));
  await assert.rejects(access(appDir), { code: "ENOENT" });
  assert.throws(() => process.kill(pid, 0), { code: "ESRCH" }, "fixture process must exit before removing its store");
}

try {
  await test("each run gets a fresh store; completed runs discard every artifact", async () => {
    const { binary, marker } = await fakeGateway();
    const dirs = new Set();
    for (let i = 0; i < 2; i++) {
      await withDisposableGateway(binary, async ({ appDir, baseUrl, setupToken }) => {
        assert.ok(!dirs.has(appDir));
        dirs.add(appDir);
        assert.match(baseUrl, /^http:\/\/127\.0\.0\.1:\d+$/);
        const config = JSON.parse(await readFile(path.join(appDir, "config.json"), "utf8"));
        assert.equal(config.setup_token, setupToken);
        assert.equal(config.source_of_truth, "config.json");
        assert.deepEqual(config.providers, {}, "do not auto-detect real provider keys from the caller's environment");
        assert.equal(config.config_store.config.path, path.join(appDir, "config.db"));
        assert.equal(config.logs_store.enabled, false);
        assert.equal(config.plugins, undefined, "telemetry must not be preseeded");
        for (const key of ["pricing_url", "model_parameters_url"]) {
          assert.equal(fileURLToPath(config.framework.pricing[key]), path.join(appDir, "empty-datasheet.json"));
        }
        await assert.rejects(access(config.config_store.config.path), { code: "ENOENT" });
        await writeFile(config.config_store.config.path, "simulated persisted telemetry");
      });
      await assertDiscarded(marker);
    }
  });
  await test("a failing regression still stops its gateway and discards the store", async () => {
    const { binary, marker } = await fakeGateway();
    await assert.rejects(withDisposableGateway(binary, async () => { throw new Error("regression failed"); }), /regression failed/);
    await assertDiscarded(marker);
  });
  for (const [mode, message] of [["exit", /exited/], ["hang", /timed out/], ["unauthorized", /setup token/], ["dirty", /no persisted telemetry/], ["resolver-dirty", /no persisted telemetry or resolver/]]) {
    await test(`startup ${mode} fails closed and cleans up`, async () => {
      const { binary, marker } = await fakeGateway(mode);
      await assert.rejects(withDisposableGateway(binary, async () => { assert.fail("must not run Newman"); }, { startupTimeoutMs: 1200 }), message);
      await assertDiscarded(marker);
    });
  }
  await test("interrupting startup cancels readiness and cleans up", async () => {
    const { binary, marker } = await fakeGateway("hang");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("test interruption")), 500);
    try {
      await assert.rejects(withDisposableGateway(binary, async () => assert.fail(), { signal: controller.signal }), /test interruption/);
    } finally { clearTimeout(timer); }
    await assertDiscarded(marker);
  });
  await test("a missing binary does not leave a temporary directory", async () => {
    const before = (await readdir(tmpdir())).filter((name) => name.startsWith("bifrost-plugin-ordering-")).sort();
    await assert.rejects(withDisposableGateway(path.join(testDir, "missing"), async () => assert.fail()), /exited/);
    assert.deepEqual((await readdir(tmpdir())).filter((name) => name.startsWith("bifrost-plugin-ordering-")).sort(), before);
  });
  for (const newmanExit of [0, 1]) {
    await test(`CLI propagates Newman exit ${newmanExit} and discards the store`, async () => {
      const { binary, marker } = await fakeGateway();
      const newmanVerified = path.join(testDir, `newman-verified-${passed}`);
      await writeFile(path.join(testDir, "newman"), `#!${process.execPath}
const fs = require('node:fs');
const assert = require('node:assert/strict');
const args = process.argv.slice(2);
assert.equal(args[0], 'run');
const collection = JSON.parse(fs.readFileSync(args[1], 'utf8'));
assert.equal(collection.item.length, 1);
assert.equal(collection.item[0].item.length, 9);
assert.equal(collection.event, undefined);
const environment = JSON.parse(fs.readFileSync(args[args.indexOf('-e') + 1], 'utf8'));
assert.equal(environment.values.find(row => row.key === 'builtinPluginOrderingFixture').value, '1');
for (const key of ['OPENAI_API_KEY', 'AWS_SECRET_ACCESS_KEY', 'HTTP_PROXY', 'BASE_URL', 'APP_DIR', 'BIFROST_SETUP_TOKEN']) {
  assert.equal(process.env[key], undefined, 'Newman must not inherit ' + key);
}
assert.ok(process.env.PATH, 'Newman retains the executable search path');
assert.ok(args.includes('--bail'));
fs.writeFileSync(${JSON.stringify(newmanVerified)}, 'ok');
process.exit(${newmanExit});
`, { mode: 0o700 });
      const child = spawn(process.execPath, [fileURLToPath(new URL("./run-builtin-plugin-ordering.mjs", import.meta.url)), "--binary", binary], {
        env: {
          ...process.env, PATH: `${testDir}:${process.env.PATH}`, BASE_URL: "http://shared.invalid", APP_DIR: testDir,
          OPENAI_API_KEY: "fixture-canary-openai", AWS_SECRET_ACCESS_KEY: "fixture-canary-aws",
          HTTP_PROXY: "http://fixture-canary.invalid:3128", BIFROST_SETUP_TOKEN: "fixture-canary-parent-token",
        }, stdio: "pipe",
      });
      let stderr = "";
      child.stderr.on("data", (chunk) => { stderr += chunk; });
      child.stdout.resume();
      const code = await new Promise((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
      assert.equal(code, newmanExit, stderr);
      await access(newmanVerified); // Exit 1 must come from the stub, not a failed assertion.
      const { environmentKeys } = JSON.parse(await readFile(marker, "utf8"));
      for (const key of ["OPENAI_API_KEY", "AWS_SECRET_ACCESS_KEY", "HTTP_PROXY", "BASE_URL", "APP_DIR"]) {
        assert.ok(!environmentKeys.includes(key), `gateway must not inherit ${key}`);
      }
      assert.ok(environmentKeys.includes("PATH"));
      assert.ok(environmentKeys.includes("BIFROST_SETUP_TOKEN"), "gateway receives its generated fixture credential");
      await assertDiscarded(marker);
      await access(path.join(testDir, "newman")); // Shared APP_DIR was never discarded.
    });
  }
  await test("CLI finds Newman in npm's global bin directory when it is absent from PATH", async () => {
    const { binary, marker } = await fakeGateway();
    await rm(path.join(testDir, "newman"), { force: true });
    const npmPrefix = path.join(testDir, "npm-global");
    const npmBin = path.join(npmPrefix, "bin");
    const newmanVerified = path.join(testDir, "newman-global-verified");
    await mkdir(npmBin, { recursive: true });
    await writeFile(path.join(testDir, "npm"), `#!${process.execPath}
const assert = require('node:assert/strict');
assert.deepEqual(process.argv.slice(2), ['prefix', '--global']);
for (const key of ['OPENAI_API_KEY', 'AWS_SECRET_ACCESS_KEY', 'HTTP_PROXY', 'BASE_URL', 'APP_DIR', 'BIFROST_SETUP_TOKEN']) {
  assert.equal(process.env[key], undefined, 'npm prefix lookup must not inherit ' + key);
}
console.log(${JSON.stringify(npmPrefix)});
`, { mode: 0o700 });
    await writeFile(path.join(npmBin, "newman"), `#!${process.execPath}
require('node:fs').writeFileSync(${JSON.stringify(newmanVerified)}, 'ok');
`, { mode: 0o700 });
    const child = spawn(process.execPath, [fileURLToPath(new URL("./run-builtin-plugin-ordering.mjs", import.meta.url)), "--binary", binary], {
      env: {
        ...process.env, PATH: testDir, BASE_URL: "http://shared.invalid", APP_DIR: testDir,
        OPENAI_API_KEY: "fixture-canary-openai", AWS_SECRET_ACCESS_KEY: "fixture-canary-aws",
        HTTP_PROXY: "http://fixture-canary.invalid:3128", BIFROST_SETUP_TOKEN: "fixture-canary-parent-token",
      }, stdio: "pipe",
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.stdout.resume();
    const code = await new Promise((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    assert.equal(code, 0, stderr);
    await access(newmanVerified);
    await assertDiscarded(marker);
  });
  // Real npm must read the same config used by installation. A constant-output
  // npm stub cannot catch an environment filter silently changing its prefix.
  const npmPaths = await Promise.all((process.env.PATH || "").split(path.delimiter).filter(Boolean).map(async (directory) => {
    const candidate = path.resolve(directory, "npm");
    try { await access(candidate, constants.X_OK); return await realpath(candidate); } catch { return null; }
  }));
  const npmCLI = npmPaths.find(Boolean);
  assert.ok(npmCLI, "the dev container needs npm to exercise real global-prefix resolution");
  for (const source of ["NPM_CONFIG_PREFIX", "npm_config_prefix", "NPM_CONFIG_USERCONFIG", "npm_config_userconfig", "HOME", "NPM_CONFIG_GLOBALCONFIG", "npm_config_globalconfig", "precedence", "PREFIX", "DESTDIR"]) {
    await test(`CLI honors real npm configuration from ${source} without leaking it to fixture children`, async () => {
      const { binary, marker } = await fakeGateway();
      const fixture = path.join(testDir, `npm-config-${passed}`);
      const bin = path.join(fixture, "bin");
      const npmHome = path.join(fixture, "home");
      const prefix = path.join(fixture, "global packages");
      const userconfig = path.join(fixture, "user.npmrc");
      const globalconfig = path.join(fixture, "global.npmrc");
      const verified = path.join(fixture, "newman-verified");
      for (const directory of [bin, npmHome, path.join(prefix, "bin")]) await mkdir(directory, { recursive: true });
      await symlink(process.execPath, path.join(bin, "node"));
      await symlink(npmCLI, path.join(bin, "npm"));
      await writeFile(userconfig, "");
      await writeFile(globalconfig, "");
      const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/^npm_config_/i.test(key) && !["PREFIX", "DESTDIR"].includes(key)));
      Object.assign(env, {
        PATH: bin, HOME: npmHome, NPM_CONFIG_USERCONFIG: userconfig, NPM_CONFIG_GLOBALCONFIG: globalconfig,
        OPENAI_API_KEY: "fixture-canary-openai", AWS_SECRET_ACCESS_KEY: "fixture-canary-aws",
        HTTP_PROXY: "http://fixture-canary.invalid:3128", BASE_URL: "http://shared.invalid", APP_DIR: fixture,
        BIFROST_SETUP_TOKEN: "fixture-canary-parent-token",
      });
      if (source === "HOME") {
        delete env.NPM_CONFIG_USERCONFIG;
        await writeFile(path.join(npmHome, ".npmrc"), `prefix=${prefix}\n`);
      } else if (/userconfig/i.test(source)) {
        delete env.NPM_CONFIG_USERCONFIG;
        env[source] = userconfig;
        await writeFile(userconfig, `prefix=${prefix}\n`);
      } else if (/globalconfig/i.test(source)) {
        delete env.NPM_CONFIG_GLOBALCONFIG;
        env[source] = globalconfig;
        await writeFile(globalconfig, `prefix=${prefix}\n`);
      } else if (["PREFIX", "DESTDIR"].includes(source)) {
        env[source] = prefix;
        // Some npm distributions override their default prefix in a bundled
        // npmrc. Reference these settings explicitly to exercise them on both.
        await writeFile(userconfig, "prefix=${" + source + "}\n");
      } else {
        env[source === "precedence" ? "NPM_CONFIG_PREFIX" : source] = prefix;
        if (source === "precedence") await writeFile(userconfig, `prefix=${path.join(fixture, "wrong-prefix")}\n`);
      }
      // This is the prefix npm install --global uses, before the runner filters env.
      const { stdout } = await execFileAsync(path.join(bin, "npm"), ["prefix", "--global"], { env, cwd: fixture });
      assert.equal(stdout.trim(), prefix);
      await writeFile(path.join(prefix, "bin", "newman"), `#!${process.execPath}
const assert = require('node:assert/strict');
for (const key of ['HOME', 'PREFIX', 'DESTDIR', 'OPENAI_API_KEY', 'AWS_SECRET_ACCESS_KEY', 'HTTP_PROXY', 'BASE_URL', 'APP_DIR', 'BIFROST_SETUP_TOKEN']) {
  assert.equal(process.env[key], undefined, 'Newman must not inherit ' + key);
}
assert.ok(!Object.keys(process.env).some(key => /^npm_config_/i.test(key)));
require('node:fs').writeFileSync(${JSON.stringify(verified)}, 'ok');
`, { mode: 0o700 });
      await execFileAsync(process.execPath, [runnerPath, "--binary", binary], { env, cwd: fixture, timeout: 10000 });
      await access(verified);
      const { environmentKeys } = JSON.parse(await readFile(marker, "utf8"));
      assert.ok(!environmentKeys.some(key => ["HOME", "PREFIX", "DESTDIR"].includes(key) || /^npm_config_/i.test(key)));
      for (const key of ["OPENAI_API_KEY", "AWS_SECRET_ACCESS_KEY", "HTTP_PROXY", "BASE_URL", "APP_DIR"]) {
        assert.ok(!environmentKeys.includes(key), `gateway must not inherit ${key}`);
      }
      await assertDiscarded(marker);
    });
  }
  for (const interruption of ["SIGTERM", "SIGINT", "timeout"]) {
    await test(`npm lookup ${interruption} stops its child before any gateway starts`, async () => {
      const { binary, marker } = await fakeGateway();
      const bin = path.join(testDir, `npm-hang-${passed}`);
      const npmMarker = path.join(bin, "pid");
      await mkdir(bin);
      await writeFile(path.join(bin, "npm"), `#!${process.execPath}
require('node:fs').writeFileSync(${JSON.stringify(npmMarker)}, String(process.pid));
process.on('SIGTERM', () => {});
setInterval(() => {}, 1000);
`, { mode: 0o700 });
      const child = spawn(process.execPath, [runnerPath, "--binary", binary], {
        env: { ...process.env, PATH: bin }, stdio: "pipe",
      });
      let stderr = "";
      child.stdout.resume();
      child.stderr.on("data", (chunk) => { stderr += chunk; });
      const done = new Promise((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
      const deadline = setTimeout(() => child.kill("SIGKILL"), 10000);
      let npmPID;
      try {
        for (let i = 0; i < 100; i++) {
          try { npmPID = Number(await readFile(npmMarker, "utf8")); break; } catch (error) {
            if (error.code !== "ENOENT") throw error;
          }
          await delay(20);
        }
        assert.ok(npmPID, "npm lookup must start before interruption");
        if (interruption !== "timeout") child.kill(interruption);
        assert.equal(await done, 1, stderr);
        assert.throws(() => process.kill(npmPID, 0), { code: "ESRCH" }, "npm lookup must not survive the runner");
        await assert.rejects(access(marker), { code: "ENOENT" });
      } finally {
        child.kill("SIGKILL");
        await done;
        if (npmPID) { try { process.kill(npmPID, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; } }
        clearTimeout(deadline);
      }
    });
  }
  for (const failure of ["no-npm", "npm-exit", "empty-prefix", "relative-prefix", "missing-newman", "non-executable", "directory"]) {
    await test(`npm lookup rejects ${failure} before starting a gateway`, async () => {
      const { binary, marker } = await fakeGateway();
      const bin = path.join(testDir, `npm-invalid-${passed}`);
      const prefix = path.join(bin, "global");
      await mkdir(path.join(prefix, "bin"), { recursive: true });
      if (failure !== "no-npm") {
        const output = failure === "empty-prefix" ? "" : failure === "relative-prefix" ? "relative" : prefix;
        await writeFile(path.join(bin, "npm"), `#!${process.execPath}\n${failure === "npm-exit" ? "process.exit(2)" : `console.log(${JSON.stringify(output)})`};\n`, { mode: 0o700 });
      }
      if (failure === "non-executable") await writeFile(path.join(prefix, "bin", "newman"), "not executable", { mode: 0o600 });
      if (failure === "directory") await mkdir(path.join(prefix, "bin", "newman"));
      await assert.rejects(execFileAsync(process.execPath, [runnerPath, "--binary", binary], {
        env: { ...process.env, PATH: bin }, timeout: 10000,
      }), (error) => error.code === 1 && /Newman is unavailable|Unable to locate npm|invalid global prefix/.test(error.stderr));
      await assert.rejects(access(marker), { code: "ENOENT" }, "dependency resolution must finish before starting a gateway");
    });
  }
  await test("SIGTERM during Newman shuts down both children and discards the store", async () => {
    const { binary, marker } = await fakeGateway();
    const newmanMarker = path.join(testDir, "newman-pid");
    await writeFile(path.join(testDir, "newman"), `#!${process.execPath}
require('node:fs').writeFileSync(${JSON.stringify(newmanMarker)}, String(process.pid));
setInterval(() => {}, 1000);
`, { mode: 0o700 });
    const child = spawn(process.execPath, [fileURLToPath(new URL("./run-builtin-plugin-ordering.mjs", import.meta.url)), "--binary", binary], {
      env: { ...process.env, PATH: `${testDir}:${process.env.PATH}` }, stdio: "ignore",
    });
    const done = new Promise((resolve, reject) => { child.once("error", reject); child.once("close", resolve); });
    const timeout = setTimeout(() => child.kill("SIGKILL"), 10000);
    try {
      let newmanPID;
      for (let i = 0; i < 100; i++) {
        try { newmanPID = Number(await readFile(newmanMarker, "utf8")); break; } catch (error) {
          if (error.code !== "ENOENT") throw error;
        }
        await delay(50);
      }
      assert.ok(newmanPID, "fake Newman must start before interruption");
      child.kill("SIGTERM");
      assert.equal(await done, 1);
      assert.throws(() => process.kill(newmanPID, 0), { code: "ESRCH" });
      await assertDiscarded(marker);
    } finally {
      child.kill("SIGTERM");
      await done;
      clearTimeout(timeout);
    }
  });
  console.log(`${passed} lifecycle tests passed`);
} finally {
  await rm(testDir, { recursive: true, force: true });
}
