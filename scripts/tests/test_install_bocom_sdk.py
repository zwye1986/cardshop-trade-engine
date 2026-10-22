"""离线验证 SDK 校验与安装契约；不向用户本地 Maven 仓库写入。"""
import os
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'scripts/install-bocom-sdk.sh'
EXPECTED_SHA = '8df82ae2931dd5cc52fcbbea5643cd6819412f27f9cf64478b476c2e53120fad'


class InstallBocomSdkTest(unittest.TestCase):
    def test_distribution_directory_resolves_official_jar(self):
        distribution = self.path / 'bank package'
        (distribution / 'lib').mkdir(parents=True)
        jar = distribution / 'lib/bocom-openapi-sdk-2.2.6.jar'
        jar.write_bytes(b'controlled fixture')
        shasum = self.path / 'shasum'
        shasum.write_text('#!/bin/sh\nprintf "%s  %s\\n" "' + EXPECTED_SHA + '" "$3"\n')
        shasum.chmod(0o755)
        result = self.run_script(distribution)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('-Dfile=' + str(jar), self.capture.read_text().splitlines())

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = pathlib.Path(self.tmp.name)
        self.jar = self.path / 'official sdk.jar'
        self.jar.write_bytes(b'not the official artifact')
        self.capture = self.path / 'maven-arguments'
        mvn = self.path / 'mvn'
        mvn.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$CAPTURE"\n')
        mvn.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.path) + ':' + os.environ['PATH'], CAPTURE=str(self.capture))

    def run_script(self, *args):
        self.assertTrue(SCRIPT.is_file(), 'SDK 安装脚本尚未实现')
        return subprocess.run(['bash', str(SCRIPT), *map(str, args)], env=self.env, text=True, capture_output=True)

    def test_missing_or_mismatched_artifact_never_invokes_maven(self):
        for jar in [self.path / 'missing.jar', self.jar]:
            with self.subTest(jar=jar):
                result = self.run_script(jar)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.capture.exists())

    def test_verified_artifact_installs_explicit_pom_without_generated_dependencies(self):
        # 用摘要工具替身模拟已核实的私有 SDK，避免测试依赖仓库外的分发包。
        shasum = self.path / 'shasum'
        shasum.write_text('#!/bin/sh\nprintf "%s  %s\\n" "' + EXPECTED_SHA + '" "$3"\n')
        shasum.chmod(0o755)
        result = self.run_script(self.jar)
        self.assertEqual(result.returncode, 0, result.stderr)
        args = self.capture.read_text().splitlines()
        self.assertIn('org.apache.maven.plugins:maven-install-plugin:3.1.3:install-file', args)
        self.assertIn('-Dfile=' + str(self.jar), args)
        self.assertIn('-DpomFile=' + str(ROOT / 'scripts/bocom-openapi-sdk.pom.xml'), args)
        self.assertIn('-DgeneratePom=false', args)


if __name__ == '__main__':
    unittest.main()
