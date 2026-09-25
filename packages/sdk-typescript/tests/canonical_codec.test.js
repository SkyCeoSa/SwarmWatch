import { test, describe } from "node:test";
import assert from "node:assert/strict";
import * as crypto from "node:crypto";
import {
  CanonicalCodec,
  CanonicalDomainTag,
  CanonicalEncodingError,
  globalCanonicalCodec,
} from "../src/canonical.ts";

describe("Seam 50: Canonical Causal Encoding & Byte-Deterministic Contract Engine (ADR 0061)", () => {
  test("Key sorting and whitespace elimination matching RFC 8785", () => {
    const payload = {
      zeta: 1,
      alpha: 2,
      beta: { d: 4, c: 3 },
    };
    const encoded = CanonicalCodec.canonicalize(payload);
    assert.equal(encoded, '{"alpha":2,"beta":{"c":3,"d":4},"zeta":1}');
  });

  test("UTF-16 code unit key sorting order", () => {
    const payload = {
      "\u007e": "tilde",
      "\u00e9": "e-acute",
      "\u0100": "A-macron",
      a: "latin-a",
    };
    const encoded = CanonicalCodec.canonicalize(payload);
    assert.equal(encoded, '{"a":"latin-a","~":"tilde","\u00e9":"e-acute","\u0100":"A-macron"}');
  });

  test("Float formatting, negative zero normalization, and safe integers", () => {
    assert.equal(CanonicalCodec.canonicalize(-0), "0");
    assert.equal(CanonicalCodec.canonicalize(0), "0");

    assert.equal(CanonicalCodec.canonicalize(1.0), "1");
    assert.equal(CanonicalCodec.canonicalize(100.0), "100");
    assert.equal(CanonicalCodec.canonicalize(-42.0), "-42");

    assert.equal(CanonicalCodec.canonicalize(1.5), "1.5");
    assert.equal(CanonicalCodec.canonicalize(-1.5), "-1.5");
    assert.equal(CanonicalCodec.canonicalize(123.456), "123.456");

    assert.equal(CanonicalCodec.canonicalize(1e-5), "0.00001");
    assert.equal(CanonicalCodec.canonicalize(1e-6), "0.000001");
    assert.equal(CanonicalCodec.canonicalize(1e-7), "1e-7");
  });

  test("Non-finite numbers rejection", () => {
    assert.throws(() => CanonicalCodec.canonicalize(NaN), CanonicalEncodingError);
    assert.throws(() => CanonicalCodec.canonicalize(Infinity), CanonicalEncodingError);
    assert.throws(() => CanonicalCodec.canonicalize(-Infinity), CanonicalEncodingError);
  });

  test("Unicode preservation unescaped", () => {
    const payload = { persian: "سلام دنیا", emoji: "⚡", german: "münchen" };
    const canonical = CanonicalCodec.canonicalize(payload);
    assert.ok(canonical.includes("سلام دنیا"));
    assert.ok(canonical.includes("⚡"));
    assert.ok(canonical.includes("münchen"));
    assert.ok(!canonical.includes("\\u"));
  });

  test("Circular reference guard", () => {
    const circularArr = [1, 2];
    circularArr.push(circularArr);
    assert.throws(() => CanonicalCodec.canonicalize(circularArr), CanonicalEncodingError);

    const circularObj = { a: 1 };
    circularObj.self = circularObj;
    assert.throws(() => CanonicalCodec.canonicalize(circularObj), CanonicalEncodingError);
  });

  test("Objects with toDict or toJSON methods", () => {
    const objWithToDict = {
      agent: "agent-01",
      score: 99.5,
      toDict() {
        return { agent: this.agent, score: this.score };
      },
    };
    const canonical = CanonicalCodec.canonicalize(objWithToDict);
    assert.equal(canonical, '{"agent":"agent-01","score":99.5}');

    const objWithToJSON = {
      name: "item",
      toJSON() {
        return { name: "serialized" };
      },
    };
    assert.equal(CanonicalCodec.canonicalize(objWithToJSON), '{"name":"serialized"}');
  });

  test("Domain separation hash cross-runtime parity with Python", () => {
    const payload = { turn: 1, agent: "worker-1" };
    const canonicalBytes = CanonicalCodec.encode(payload);
    assert.equal(canonicalBytes.toString("utf8"), '{"agent":"worker-1","turn":1}');

    // Raw hash
    const digestRaw = CanonicalCodec.hash(payload);
    const expectedRaw = crypto.createHash("sha256").update(canonicalBytes).digest("hex");
    assert.equal(digestRaw, expectedRaw);

    // Tagged hash
    const digestTagged = CanonicalCodec.hash(payload, CanonicalDomainTag.LEAF);
    const prefix = Buffer.from("sw:leaf:v1\0", "utf8");
    const expectedTagged = crypto.createHash("sha256").update(prefix).update(canonicalBytes).digest("hex");
    assert.equal(digestTagged, expectedTagged);
    assert.notEqual(digestRaw, digestTagged);
  });

  test("Validate helper function", () => {
    const valid = { x: [1, 2, 3], y: null, z: true };
    const res1 = CanonicalCodec.validate(valid);
    assert.equal(res1.isValid, true);
    assert.equal(res1.error, null);

    const invalid = { bad: NaN };
    const res2 = CanonicalCodec.validate(invalid);
    assert.equal(res2.isValid, false);
    assert.ok(res2.error.includes("Non-finite"));
  });
});
