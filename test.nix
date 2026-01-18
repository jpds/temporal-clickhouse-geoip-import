{
  workerApp,
  pkgs,
  ...
}:

let
  testDatasetIPv4 = pkgs.writeTextFile {
    name = "dbip-city-ipv4.csv";
    text = ''
      1.0.0.0,1.0.0.255,AU,Queensland,,South Brisbane,,-27.4767,153.017,
      1.0.1.0,1.0.3.255,CN,Fujian,,Wenquan,,26.0998,119.297,
      1.0.4.0,1.0.7.255,AU,Victoria,,Narre Warren,,-38.0268,145.301,
      1.0.8.0,1.0.15.255,CN,Guangdong,,Guangzhou,,23.1317,113.266,
      1.0.16.0,1.0.16.255,JP,Tokyo,,Chiyoda,,35.6916,139.768,
      1.0.17.0,1.0.31.255,JP,Tokyo,,Shinjuku (1-chome),,35.6944,139.703,
      1.0.32.0,1.0.63.255,CN,Guangdong,,Guangzhou,,23.1317,113.266,
      1.0.64.0,1.0.90.255,JP,Hiroshima,,Hiroshima,,34.4006,132.476,
      1.0.91.0,1.0.91.255,JP,Tottori,,Tottori-shi,,35.5,134.233,
    '';
  };

  testDatasetIPv6 = pkgs.writeTextFile {
    name = "dbip-city-ipv6.csv";
    text = ''
      2000::,2000:ffff:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
      2001::,2001::ffff:ffff:ffff:ffff:ffff:ffff,US,California,,Los Angeles (Playa Vista),,33.9829,-118.405,
      2001:1::,2001:1:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
      2001:2::,2001:2::ffff:ffff:ffff:ffff:ffff,US,Ohio,,Springfield,,39.9242,-83.8088,
      2001:2:1::,2001:2:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
      2001:3::,2001:3:ffff:ffff:ffff:ffff:ffff:ffff,US,California,,Los Angeles (Playa Vista),,33.9829,-118.405,
      2001:4::,2001:4:111:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
      2001:4:112::,2001:4:112:ffff:ffff:ffff:ffff:ffff,NL,North Holland,,Amsterdam,,52.3676,4.90414,
      2001:4:113::,2001:4:ffff:ffff:ffff:ffff:ffff:ffff,CH,Fribourg,,Murten/Morat,,46.9219,7.16595,
      2001:5::,2001:5::ffff:ffff:ffff:ffff:ffff,NL,North Holland,,Amsterdam (Amsterdam-Centrum),,52.3783,4.90073,
    '';
  };

  testDatasetIPv4Compressed =
    pkgs.runCommand "dbip-city-ipv4.csv.gz"
      {
        buildInputs = [ pkgs.gzip ];
        src = testDatasetIPv4;
      }
      ''
        gzip -c $src > $out
      '';

  testDatasetIPv6Compressed =
    pkgs.runCommand "dbip-city-ipv6.csv.gz"
      {
        buildInputs = [ pkgs.gzip ];
        src = testDatasetIPv6;
      }
      ''
        gzip -c $src > $out
      '';
in
{
  name = "temporal-clichouse-geoip-import";

  nodes = {
    clickhouse =
      { pkgs, ... }:
      {
        networking.firewall.allowedTCPPorts = [ 8123 ];

        environment.etc = {
          "clickhouse-server/config.d/listen.xml".text = ''
            <clickhouse>
              <listen_host>::</listen_host>
            </clickhouse>
          '';

          "clickhouse-server/users.d/geoip-import.xml".text = ''
            <clickhouse>
              <users>
                <geoip-import>
                  <password>testpassword</password>

                  <access_management>0</access_management>

                  <quota>default</quota>
                  <default_database>geoip</default_database>

                  <grants>
                    <query>GRANT CREATE TABLE ON geoip.*</query>
                    <query>GRANT DROP TABLE ON geoip.*</query>
                    <query>GRANT SELECT ON geoip.*</query>
                    <query>GRANT INSERT ON geoip.*</query>
                  </grants>
                </geoip-import>
              </users>
            </clickhouse>
          '';
        };

        services.clickhouse = {
          enable = true;
        };

        virtualisation = {
          diskSize = 10 * 1024;
          memorySize = 2 * 1024;
        };
      };

    caddy =
      { pkgs, ... }:
      {
        networking.firewall.allowedTCPPorts = [ 80 ];

        services.caddy = {
          enable = true;
          extraConfig = ''
            http://caddy {
              encode gzip

              file_server
              root * ${
                pkgs.runCommand "testdir" { } ''
                  mkdir "$out"
                  cp ${testDatasetIPv4Compressed} $out/dbip-city-ipv4.csv.gz
                  cp ${testDatasetIPv6Compressed} $out/dbip-city-ipv6.csv.gz
                ''
              }
            }
          '';
        };
      };

    worker =
      { pkgs, lib, ... }:
      {
        # networking.firewall.allowedTCPPorts = [ 9198 ];

        systemd.services.temporal-clickhouse-geoip-import = {
          wantedBy = [ "multi-user.target" ];
          after = [ "network-online.target" ];
          requiredBy = [ "network-online.target" ];

          environment = {
            DOWNLOAD_HOST = "http://caddy/";

            CLICKHOUSE_DATABASE = "geoip";
            CLICKHOUSE_HOST = "clickhouse";
            CLICKHOUSE_PASSWORD = "testpassword";
            CLICKHOUSE_USERNAME = "geoip-import";

            TEMPORAL_ADDRESS = "temporal:7233";
            TEMPORAL_NAMESPACE = "clickhouse-geoip";
          };

          serviceConfig = {
            ExecStart = "${lib.getExe' workerApp "geoip-import-worker.py"}";
            User = "temporal-worker";
            Group = "temporal-worker";
            Restart = "on-failure";
            DynamicUser = true;
            CapabilityBoundingSet = [ "" ];
            LockPersonality = true;
            MemoryDenyWriteExecute = true;
            NoNewPrivileges = true;
            RestrictAddressFamilies = [ ];
            StateDirectory = "temporal-worker";
            SystemCallArchitectures = "native";
            SystemCallFilter = [
              "@system-service @resources"
              "~@privileged"
            ];
          };
        };
      };

    temporal =
      { pkgs, lib, ... }:
      {
        networking.firewall.allowedTCPPorts = [ 7233 ];

        environment.systemPackages = [
          pkgs.grpc-health-probe
          (pkgs.temporal-cli.overrideAttrs (oldAttrs: rec {
            version = "1.3.0";

            src = pkgs.fetchFromGitHub {
              owner = "temporalio";
              repo = "cli";
              tag = "v${version}";
              hash = "sha256-9O+INXJhNwgwwvC0751ifdHmxbD0qI5A3LdDb4Krk/o=";
            };

            vendorHash = "sha256-Xe/qrlqg6DpCNmsO/liTKjWIaY3KznkOQdXSSoJVZq4=";

            doCheck = false;
          }))
        ];

        services.temporal = {
          enable = true;
          settings = {
            # Based on https://github.com/temporalio/temporal/blob/main/config/development-sqlite.yaml
            log = {
              stdout = true;
              level = "info";
            };
            services = {
              frontend = {
                rpc = {
                  grpcPort = 7233;
                  membershipPort = 6933;
                  bindOnIP = "0.0.0.0";
                  httpPort = 7243;
                };
              };
              matching = {
                rpc = {
                  grpcPort = 7235;
                  membershipPort = 6935;
                  bindOnLocalHost = true;
                };
              };
              history = {
                rpc = {
                  grpcPort = 7234;
                  membershipPort = 6934;
                  bindOnLocalHost = true;
                };
              };
              worker = {
                rpc = {
                  grpcPort = 7239;
                  membershipPort = 6939;
                  bindOnLocalHost = true;
                };
              };
            };

            global = {
              membership = {
                maxJoinDuration = "30s";
                broadcastAddress = "0.0.0.0";
              };
            };

            persistence = {
              defaultStore = "sqlite-default";
              visibilityStore = "sqlite-visibility";
              numHistoryShards = 1;
              datastores = {
                sqlite-default = {
                  sql = {
                    user = "";
                    password = "";
                    pluginName = "sqlite";
                    databaseName = "default";
                    connectAddr = "localhost";
                    connectProtocol = "tcp";
                    connectAttributes = {
                      mode = "memory";
                      cache = "private";
                    };
                    maxConns = 1;
                    maxIdleConns = 1;
                    maxConnLifetime = "1h";
                    tls = {
                      enabled = false;
                      caFile = "";
                      certFile = "";
                      keyFile = "";
                      enableHostVerification = false;
                      serverName = "";
                    };
                  };
                };
                sqlite-visibility = {
                  sql = {
                    user = "";
                    password = "";
                    pluginName = "sqlite";
                    databaseName = "default";
                    connectAddr = "localhost";
                    connectProtocol = "tcp";
                    connectAttributes = {
                      mode = "memory";
                      cache = "private";
                    };
                    maxConns = 1;
                    maxIdleConns = 1;
                    maxConnLifetime = "1h";
                    tls = {
                      enabled = false;
                      caFile = "";
                      certFile = "";
                      keyFile = "";
                      enableHostVerification = false;
                      serverName = "";
                    };
                  };
                };
              };
            };
            clusterMetadata = {
              enableGlobalNamespace = false;
              failoverVersionIncrement = 10;
              masterClusterName = "active";
              currentClusterName = "active";
              clusterInformation = {
                active = {
                  enabled = true;
                  initialFailoverVersion = 1;
                  rpcName = "frontend";
                  rpcAddress = "temporal:7233";
                  httpAddress = "temporal:7243";
                };
              };
            };

            dcRedirectionPolicy = {
              policy = "noop";
            };

            archival = {
              history = {
                state = "enabled";
                enableRead = true;
                provider = {
                  filestore = {
                    fileMode = "0666";
                    dirMode = "0766";
                  };
                  gstorage = {
                    credentialsPath = "/tmp/gcloud/keyfile.json";
                  };
                };
              };
              visibility = {
                state = "enabled";
                enableRead = true;
                provider = {
                  filestore = {
                    fileMode = "0666";
                    dirMode = "0766";
                  };
                };
              };
            };

            namespaceDefaults = {
              archival = {
                history = {
                  state = "disabled";
                  URI = "file:///tmp/temporal_archival/development";
                };
                visibility = {
                  state = "disabled";
                  URI = "file:///tmp/temporal_vis_archival/development";
                };
              };
            };
          };
        };

        virtualisation.cores = 2;
      };
  };

  testScript =
    let
      createDatabaseQuery = pkgs.writeText "create-database-geoip.sql" ''
        CREATE DATABASE geoip;
      '';

      selectQueryIPv4 = pkgs.writeText "select-count-ipv4.sql" ''
        SELECT count() FROM geoip.geoip_ipv4;
      '';

      selectQueryIPv6 = pkgs.writeText "select-count-ipv6.sql" ''
        SELECT count() FROM geoip.geoip_ipv6;
      '';

      # https://clickhouse.com/blog/geolocating-ips-in-clickhouse-and-grafana
      selectBitXORIPv4 = pkgs.writeText "select-bitxor-ipv4.sql" ''
        WITH
            bitXor(ip_range_start, ip_range_end) AS xor,
            if(xor != 0, ceil(log2(xor)), 0) AS unmatched,
            32 - unmatched AS cidr_suffix,
            toIPv4(bitAnd(bitNot(pow(2, unmatched) - 1), ip_range_start)::UInt64) AS cidr_address
        SELECT
            ip_range_start,
            ip_range_end,
            concat(toString(cidr_address), '/', toString(cidr_suffix)) AS cidr
        FROM
            geoip.geoip_ipv4;
      '';

      selectBitXORIPv6 = pkgs.writeText "select-bitxor-ipv6.sql" ''
        WITH
            bitXor(ip_range_start, ip_range_end) AS xor,
            if(xor != 0, toUInt8(ceil(log2(xor))), 0) AS unmatched,
            128 - unmatched AS cidr_suffix,
            CAST(reverse(reinterpretAsFixedString(bitAnd(bitNot(bitShiftRight(toUInt128(bitNot(0)), cidr_suffix)), ip_range_start))) AS IPv6) AS cidr_address
        SELECT
            ip_range_start,
            ip_range_end,
            concat(toString(cidr_address), '/', toString(cidr_suffix)) AS cidr
        FROM
            geoip.geoip_ipv6;
      '';

      createDictionaryIPTrie = pkgs.writeText "create-dictionary-ip-trie.sql" ''
        CREATE DICTIONARY geoip.ip_trie (
           cidr String,
           latitude Float64,
           longitude Float64,
           country_code String
        )
        PRIMARY KEY cidr
        SOURCE(CLICKHOUSE(TABLE 'geoip'))
        LAYOUT(ip_trie)
        LIFETIME(3600);
      '';

      selectTrie = pkgs.writeText "select-dictionary-ip-trie.sql" ''
        SELECT * FROM geoip.ip_trie;
      '';

      getDictionaryIPTrieIPv4 = pkgs.writeText "get-dictionary-ip-trie-ipv4.sql" ''
        SELECT dictGet(
           'geoip.ip_trie',
           ('country_code', 'latitude', 'longitude'),
           tuple('1.0.4.8'::IPv4)
        );
      '';

      getDictionaryIPTrieIPv6 = pkgs.writeText "get-dictionary-ip-trie-ipv6.sql" ''
        SELECT dictGet(
           'geoip.ip_trie',
           ('country_code', 'latitude', 'longitude'),
           tuple('2001:3::90'::IPv6)
        );
      '';
    in
    ''
      caddy.start()
      caddy.wait_for_unit("caddy")
      caddy.wait_for_open_port(80)

      clickhouse.start()
      clickhouse.wait_for_unit("clickhouse")
      clickhouse.wait_for_open_port(8123)

      clickhouse.wait_until_succeeds(
        "cat ${createDatabaseQuery} | clickhouse-client"
      )

      temporal.start()
      temporal.wait_for_unit("temporal")
      temporal.wait_for_open_port(6933)
      temporal.wait_for_open_port(6934)
      temporal.wait_for_open_port(6935)
      temporal.wait_for_open_port(7233)
      temporal.wait_for_open_port(7234)
      temporal.wait_for_open_port(7235)

      temporal.wait_until_succeeds(
        "grpc-health-probe -addr=temporal:7233 -service=temporal.api.workflowservice.v1.WorkflowService"
      )

      temporal.wait_until_succeeds(
        "grpc-health-probe -addr=localhost:7234 -service=temporal.api.workflowservice.v1.HistoryService"
      )

      temporal.wait_until_succeeds(
        "grpc-health-probe -addr=localhost:7235 -service=temporal.api.workflowservice.v1.MatchingService"
      )

      temporal.wait_until_succeeds(
        "journalctl -o cat -u temporal.service | grep 'server-version' | grep '${pkgs.temporal.version}'"
      )

      temporal.wait_until_succeeds(
        "journalctl -o cat -u temporal.service | grep 'Frontend is now healthy'"
      )

      temporal.log(temporal.wait_until_succeeds("temporal operator namespace create --namespace clickhouse-geoip", timeout=60))

      worker.start()
      worker.systemctl("start network-online.target")
      worker.wait_for_unit("network-online.target")
      worker.wait_for_unit("temporal-clickhouse-geoip-import")

      temporal.wait_until_succeeds(
        """
          temporal workflow start --namespace clickhouse-geoip --type ClickHouseGeoIPImport --workflow-id clickhouse-geoip-import --task-queue clickhouse-geoip-import-queue
        """
      )

      import json
      workflow_result_v4_json = json.loads(temporal.wait_until_succeeds("temporal workflow result --namespace clickhouse-geoip -w clickhouse-geoip-raw-insert-ipv4 --output json", timeout=60))
      assert workflow_result_v4_json['result'] == "9 records inserted for IPv4"
      assert workflow_result_v4_json['status'] == "COMPLETED"

      workflow_result_v6_json = json.loads(temporal.wait_until_succeeds("temporal workflow result --namespace clickhouse-geoip -w clickhouse-geoip-raw-insert-ipv6 --output json", timeout=60))
      assert workflow_result_v6_json['result'] == "10 records inserted for IPv6"
      assert workflow_result_v6_json['status'] == "COMPLETED"

      clickhouse.log(clickhouse.wait_until_succeeds(
        "cat ${selectQueryIPv4} | clickhouse-client | grep '9'"
      ))
      clickhouse.log(clickhouse.wait_until_succeeds(
        "cat ${selectQueryIPv6} | clickhouse-client | grep '10'"
      ))

      clickhouse.log(clickhouse.wait_until_succeeds(
        "cat ${selectBitXORIPv4} | clickhouse-client"
      ))
      clickhouse.log(clickhouse.wait_until_succeeds(
        "cat ${selectBitXORIPv6} | clickhouse-client"
      ))

      clickhouse.log(clickhouse.wait_until_succeeds(
        "cat ${createDictionaryIPTrie} | clickhouse-client"
      ))

      clickhouse.log(clickhouse.wait_until_succeeds(
        "cat ${selectTrie} | clickhouse-client"
      ))

      result = clickhouse.succeed(
        "cat ${getDictionaryIPTrieIPv4} | clickhouse-client"
      )
      assert result.strip() == "('AU',-38.0268,145.301)"

      result = clickhouse.succeed(
        "cat ${getDictionaryIPTrieIPv6} | clickhouse-client"
      )
      assert result.strip() == "('US',33.9829,-118.405)"
    '';
}
