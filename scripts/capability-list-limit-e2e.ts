import assert from "node:assert/strict";
import {
  TEAM_CURRENT_CAPABILITIES,
  teamCapabilitiesForProtocol,
  teamCapabilitiesVisibleToProtocol,
} from "@openbot/contracts/team-protocol/current";

for (const protocol of [7, 8]) {
  const sent = teamCapabilitiesForProtocol(protocol);
  assert.ok(
    sent.length <= 128,
    `protocol ${protocol} client sends ${sent.length} capabilities; a v7/v8 host reads at most 128`,
  );
  const visible = teamCapabilitiesVisibleToProtocol([...TEAM_CURRENT_CAPABILITIES], protocol);
  assert.ok(visible.length <= 128, `protocol ${protocol} host lists ${visible.length} capabilities to an older peer`);
}
const protocol9: readonly string[] = teamCapabilitiesForProtocol(9);
for (const capability of ["shared-directory-v1", "context-prs-v1", "watch-registry-v1"])
  assert.ok(protocol9.includes(capability), `protocol 9 keeps ${capability}`);
process.stdout.write("capability list limits hold for protocols 7, 8 and 9\n");
