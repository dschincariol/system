from __future__ import annotations

import json
import os
import subprocess
import textwrap
from io import StringIO
from pathlib import Path

from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[1]


def test_operator_env_parser_matches_python_dotenv_quote_and_comment_semantics():
    env_text = textwrap.dedent(
        r"""
        TS_PG_DSN="host=127.0.0.1 port=5432 user=trading dbname=trading"
        SINGLE='single # not comment = ok'
        UNQUOTED_COMMENT=plain # trailing comment
        UNQUOTED_HASH=plain#not-comment
        INTERNAL_EQUALS=one=two=three
        DOUBLE_ESCAPED="escaped \" quote # not comment"
        SINGLE_ESCAPED='escaped \' quote # not comment'
        QUOTED_SUFFIX="trailing"#comment
        export EXPORTED=value
        """
    ).lstrip()
    expected = dict(dotenv_values(stream=StringIO(env_text)))

    script = textwrap.dedent(
        """
        const { parseEnvText } = require("./boot/operator_env_file");
        const input = process.env.TEST_ENV_TEXT || "";
        console.log(JSON.stringify(parseEnvText(input)));
        """
    )
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        env={**os.environ, "TEST_ENV_TEXT": env_text},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == expected
    assert expected["TS_PG_DSN"] == "host=127.0.0.1 port=5432 user=trading dbname=trading"
    assert expected["INTERNAL_EQUALS"] == "one=two=three"
    assert expected["UNQUOTED_COMMENT"] == "plain"
    assert expected["UNQUOTED_HASH"] == "plain#not-comment"


def test_operator_env_serialize_round_trips_values_that_need_protection():
    payload = {
        "TS_PG_DSN": "host=127.0.0.1 port=5432 user=trading dbname=trading",
        "INTERNAL_EQUALS": "one=two=three",
        "WRAPPED_LITERAL": '"literal quotes stay data"',
        "HASH_COMMENT_DATA": "value # not comment",
        "SIMPLE_HASH": "value#not-comment",
        "LEADING_SPACE": " value",
        "TRAILING_SPACE": "value ",
        "MULTILINE": "line\nnext",
        "TABBED": "a\tb",
    }
    script = textwrap.dedent(
        """
        const assert = require("node:assert/strict");
        const { parseEnvText, serializeEnv } = require("./boot/operator_env_file");
        const payload = JSON.parse(process.env.TEST_ENV_PAYLOAD);
        const serialized = serializeEnv(payload);
        const reparsed = parseEnvText(serialized);
        assert.deepEqual(reparsed, payload);
        console.log(JSON.stringify({ serialized, reparsed }));
        """
    )

    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        env={**os.environ, "TEST_ENV_PAYLOAD": json.dumps(payload)},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert "TS_PG_DSN=host=127.0.0.1 port=5432 user=trading dbname=trading" in data["serialized"]
    assert 'HASH_COMMENT_DATA="value # not comment"' in data["serialized"]
    assert 'INTERNAL_EQUALS=one=two=three' in data["serialized"]


def test_operator_server_uses_shared_env_file_parser_for_production_paths():
    text = (ROOT / "boot" / "operator_server.js").read_text(encoding="utf-8")

    assert 'require("./operator_env_file")' in text
    assert "return parseOperatorEnvText(text);" in text
    assert "return serializeOperatorEnv(obj);" in text
    assert "const v = trimmed.slice(idx + 1).trim();" not in text
