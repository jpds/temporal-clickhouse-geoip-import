{
  description = "Temporal ClickHouse GeoIP Import";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        workflowLib = pkgs.python3Packages.buildPythonPackage rec {
          pname = "temporal-clickhouse-geoip-import-lib";
          version = "1.0.0";
          pyproject = false;

          dontUnpack = true;

          installPhase = ''
            install -Dm644 ${./activities.py} $out/${pkgs.python3.sitePackages}/activities.py
            install -Dm644 ${./workflows.py}  $out/${pkgs.python3.sitePackages}/workflows.py
          '';

          propagatedBuildInputs = with pkgs.python3Packages; [
            clickhouse-connect
            requests
            temporalio
          ];

          pythonImportsCheck = [
            "activities"
            "workflows"
          ];

          meta = with pkgs.lib; {
            description = "Temporal ClickHouse GeoIP import libraries";
            license = licenses.mit;
          };
        };

        workerApp = pkgs.python3Packages.buildPythonApplication rec {
          pname = "temporal-clickhouse-geoip-import-worker";
          version = "1.0.0";
          pyproject = false;

          dontUnpack = true;

          installPhase = ''
            install -Dm755 ${./geoip-import-worker.py} $out/bin/geoip-import-worker.py
          '';

          propagatedBuildInputs = with pkgs.python3Packages; [
            temporalio
            workflowLib
          ];

          meta = with pkgs.lib; {
            description = "Temporal ClickHouse GeoIP import worker";
            license = licenses.mit;
          };
        };
      in
      {
        packages.workerApp = workerApp;
        packages.default = workerApp;

        checks.temporal-clickhouse-geoip-import = pkgs.testers.runNixOSTest (
          import ./test.nix { inherit workerApp pkgs; }
        );
      }
    );
}
