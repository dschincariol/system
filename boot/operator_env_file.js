"use strict";

function isHorizontalWhitespace(ch) {
  return ch === " " || ch === "\t" || ch === "\f" || ch === "\v";
}

function isNewline(ch) {
  return ch === "\n" || ch === "\r";
}

function skipToNextLine(text, pos) {
  while (pos < text.length && !isNewline(text[pos])) pos += 1;
  if (text[pos] === "\r" && text[pos + 1] === "\n") return pos + 2;
  if (pos < text.length && isNewline(text[pos])) return pos + 1;
  return pos;
}

function skipWhitespace(text, pos) {
  while (pos < text.length && /\s/.test(text[pos])) pos += 1;
  return pos;
}

function skipHorizontalWhitespace(text, pos) {
  while (pos < text.length && isHorizontalWhitespace(text[pos])) pos += 1;
  return pos;
}

function decodeSingleQuotedValue(value) {
  return String(value || "").replace(/\\[\\']/g, (match) => match[1]);
}

function decodeDoubleQuotedValue(value) {
  const escapes = {
    "\\": "\\",
    "'": "'",
    '"': '"',
    a: "\x07",
    b: "\b",
    f: "\f",
    n: "\n",
    r: "\r",
    t: "\t",
    v: "\v"
  };
  return String(value || "").replace(/\\[\\'"abfnrtv]/g, (match) => escapes[match[1]]);
}

function parseQuotedValue(text, pos, quote) {
  let value = "";
  let i = pos + 1;
  while (i < text.length) {
    const ch = text[i];
    if (ch === "\\") {
      const next = text[i + 1];
      if (quote === "'" && (next === "\\" || next === "'")) {
        value += ch + next;
        i += 2;
        continue;
      }
      if (quote === '"' && next === '"') {
        value += ch + next;
        i += 2;
        continue;
      }
      value += ch;
      i += 1;
      continue;
    }
    if (ch === quote) {
      return {
        ok: true,
        value: quote === "'" ? decodeSingleQuotedValue(value) : decodeDoubleQuotedValue(value),
        pos: i + 1
      };
    }
    value += ch;
    i += 1;
  }
  return { ok: false, value: "", pos: i };
}

function parseUnquotedValue(text, pos) {
  let i = pos;
  while (i < text.length && !isNewline(text[i])) i += 1;
  const raw = text.slice(pos, i);
  return {
    value: raw.replace(/\s+#.*/, "").replace(/[^\S\r\n]+$/, ""),
    pos: i
  };
}

function consumeQuotedValueSuffix(text, pos) {
  let i = skipHorizontalWhitespace(text, pos);
  if (text[i] === "#") return { ok: true, pos: skipToNextLine(text, i) };
  i = skipHorizontalWhitespace(text, i);
  if (i >= text.length) return { ok: true, pos: i };
  if (text[i] === "\r" && text[i + 1] === "\n") return { ok: true, pos: i + 2 };
  if (isNewline(text[i])) return { ok: true, pos: i + 1 };
  return { ok: false, pos: skipToNextLine(text, i) };
}

function parseEnvText(text) {
  const source = String(text || "");
  const out = {};
  let pos = 0;

  while (pos < source.length) {
    pos = skipWhitespace(source, pos);
    if (pos >= source.length) break;
    if (source[pos] === "#") {
      pos = skipToNextLine(source, pos);
      continue;
    }

    if (source.startsWith("export", pos)) {
      const afterExport = source[pos + 6];
      if (afterExport && !isNewline(afterExport) && /\s/.test(afterExport)) {
        pos = skipHorizontalWhitespace(source, pos + 6);
      }
    }

    let key = "";
    if (source[pos] === "'") {
      const end = source.indexOf("'", pos + 1);
      if (end === -1) {
        pos = skipToNextLine(source, pos);
        continue;
      }
      key = source.slice(pos + 1, end);
      pos = end + 1;
    } else {
      const start = pos;
      while (
        pos < source.length &&
        source[pos] !== "=" &&
        source[pos] !== "#" &&
        !/\s/.test(source[pos])
      ) {
        pos += 1;
      }
      key = source.slice(start, pos);
    }

    pos = skipHorizontalWhitespace(source, pos);
    if (!key || source[pos] !== "=") {
      pos = skipToNextLine(source, pos);
      continue;
    }

    pos = skipHorizontalWhitespace(source, pos + 1);
    let parsed;
    const quote = source[pos];
    if (quote === "'" || quote === '"') {
      parsed = parseQuotedValue(source, pos, quote);
      if (!parsed.ok) {
        pos = skipToNextLine(source, pos);
        continue;
      }
      const suffix = consumeQuotedValueSuffix(source, parsed.pos);
      if (!suffix.ok) {
        pos = suffix.pos;
        continue;
      }
      pos = suffix.pos;
    } else {
      parsed = parseUnquotedValue(source, pos);
      pos = skipToNextLine(source, parsed.pos);
    }

    out[key] = parsed.value;
  }

  return out;
}

function envValueNeedsQuoting(value) {
  const v = String(value);
  return (
    v.startsWith(" ") ||
    v.startsWith("\t") ||
    v.startsWith("'") ||
    v.startsWith('"') ||
    /[^\S\r\n]$/.test(v) ||
    /\s+#/.test(v) ||
    /[\r\n\x07\x08\f\t\v]/.test(v)
  );
}

function serializeEnvValue(value) {
  const v = String(value);
  if (!envValueNeedsQuoting(v)) return v;
  return `"${v
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\t/g, "\\t")
    .replace(/\f/g, "\\f")
    .replace(/\v/g, "\\v")
    .replace(/\x07/g, "\\a")
    .replace(/\x08/g, "\\b")}"`;
}

function serializeEnv(obj) {
  return Object.entries(obj || {})
    .map(([k, v]) => `${k}=${serializeEnvValue(v)}`)
    .join("\n");
}

module.exports = {
  parseEnvText,
  serializeEnv,
  serializeEnvValue
};
