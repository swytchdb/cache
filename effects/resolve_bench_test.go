/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package effects

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	pb "github.com/swytchdb/cache/cluster/proto"
)

// decodeDAG parses the encoded DAG format from logs:
// "N<count>;T<tip_ids>;R<root_ids>;[id:kind:val:deps;...]"
func decodeDAG(encoded string) *DAG {
	parts := strings.Split(encoded, ";")

	type nodeInfo struct {
		id   int
		kind byte
		val  int64
		deps []int
	}
	var nodes []nodeInfo

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "N") && !strings.Contains(part, ":") {
			continue // count header
		}
		if strings.HasPrefix(part, "T") || strings.HasPrefix(part, "R") {
			continue // tips/roots headers
		}

		fields := strings.SplitN(part, ":", 4)
		if len(fields) < 4 {
			continue
		}

		id, _ := strconv.Atoi(fields[0])
		kind := fields[1][0]
		val, _ := strconv.ParseInt(fields[2], 10, 64)

		var deps []int
		if fields[3] != "" {
			for d := range strings.SplitSeq(fields[3], ",") {
				dep, _ := strconv.Atoi(d)
				deps = append(deps, dep)
			}
		}

		nodes = append(nodes, nodeInfo{id: id, kind: kind, val: val, deps: deps})
	}

	// Build DAGNodes with sequential offsets matching the encoded IDs
	dagNodes := make([]*DAGNode, len(nodes))
	for i, n := range nodes {
		offset := Tip{0, uint64(n.id)}
		effect := &pb.Effect{
			Deps: make([]*pb.EffectRef, len(n.deps)),
		}
		for j, d := range n.deps {
			effect.Deps[j] = &pb.EffectRef{NodeId: 0, Offset: uint64(d)}
		}

		switch n.kind {
		case 'D':
			effect.Kind = &pb.Effect_Data{Data: &pb.DataEffect{
				Value: &pb.DataEffect_IntVal{IntVal: n.val},
			}}
		case 'M':
			effect.Kind = &pb.Effect_Meta{Meta: &pb.MetaEffect{}}
		case 'U':
			effect.Kind = &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{}}
		case 'N':
			effect.Kind = &pb.Effect_Noop{Noop: &pb.NoopEffect{}}
		case 'B':
			effect.Kind = &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{}}
		case 'S':
			effect.Kind = &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
				State: &pb.ReducedEffect{Scalar: &pb.DataEffect{
					Value: &pb.DataEffect_IntVal{IntVal: n.val},
				}},
			}}
		case 'X':
			effect.Kind = &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{}}
		default:
			effect.Kind = &pb.Effect_Noop{Noop: &pb.NoopEffect{}}
		}

		dagNodes[i] = &DAGNode{Offset: offset, Effect: effect}

	}

	return BuildDAG(dagNodes)
}

// Real DAGs captured from production logs during redis-benchmark
var benchDAGs = map[string]string{
	"N162": `N162;T156;R157,159,161,160,158,0;0:U:0:;1:D:0:0;2:M:0:1;3:D:0:2;4:M:0:3;5:D:0:4;6:D:0:4;7:M:0:6;8:D:0:7;9:M:0:8;10:D:0:9;11:M:0:10;12:D:0:11;13:D:0:11;14:M:0:13;15:D:0:14;16:D:0:14;17:M:0:5;18:M:0:16;19:D:0:17,18;20:M:0:19;21:D:0:17,18;22:D:0:17,18;23:M:0:22;24:D:0:23;25:D:0:23;26:M:0:25;27:D:0:26;28:M:0:27;29:D:0:28;30:D:0:28;31:M:0:30;32:M:0:29;33:D:0:31,32;34:D:0:31,32;35:M:0:34;36:M:0:15;37:D:0:35,36;38:M:0:37;39:D:0:20,38;40:M:0:39;41:D:0:40;42:M:0:24;43:D:0:40;44:D:0:40;45:M:0:44;46:D:0:45;47:M:0:46;48:D:0:45;49:M:0:48;50:D:0:49;51:D:0:49;52:M:0:50;53:D:0:49;54:M:0:51;55:M:0:53;56:M:0:33;57:D:0:54;58:M:0:57;59:M:0:41;60:M:0:12;61:D:0:42,54,56,60;62:M:0:61;63:M:0:43;64:D:0:62;65:M:0:64;66:D:0:62;67:M:0:66;68:D:0:62;69:D:0:62;70:M:0:69;71:M:0:68;72:D:0:70,71;73:M:0:72;74:M:0:21;75:D:0:73,74;76:M:0:75;77:D:0:58,76;78:M:0:77;79:D:0:78;80:M:0:79;81:D:0:78;82:M:0:81;83:D:0:47,80;84:M:0:83;85:D:0:47,52,55,65,67,80;86:M:0:85;87:D:0:86;88:M:0:87;89:D:0:63,86;90:D:0:84,89;91:M:0:90;92:D:0:84,89;93:M:0:92;94:D:0:59,93;95:M:0:94;96:D:0:59,82,88,93;97:M:0:96;98:D:0:59,82,88,93;99:D:0:59,82,88,93;100:M:0:99;101:M:0:98;102:D:0:91,95,97,100,101;103:D:0:102;104:D:0:103;105:D:0:104;106:D:0:105;107:D:0:106;108:D:0:107;109:D:0:108;110:D:0:109,158;111:D:0:110;112:D:0:111;113:D:0:112,157;114:D:0:113;115:D:0:114;116:D:0:115;117:D:0:116;118:D:0:117;119:D:0:117;120:D:0:117;121:D:0:118,119,120;122:D:0:121;123:D:0:122,159,160,161;124:D:0:123;125:D:0:124;126:D:0:125;127:D:0:126;128:D:0:127;129:D:0:128;130:D:0:129;131:D:0:130;132:D:0:131;133:D:0:132;134:D:0:133;135:D:0:134;136:D:0:135;137:D:0:136;138:D:0:137;139:D:0:138;140:D:0:139;141:D:0:140;142:D:0:141;143:D:0:142;144:D:0:143;145:D:0:144;146:D:0:145;147:D:0:146;148:D:0:147;149:D:0:148;150:D:0:149,159;151:N:0:150,157;152:D:0:151;153:B:0:152;154:N:0:150,157,153;155:D:0:154;156:B:0:155;157:U:0:;158:U:0:;159:U:0:;160:U:0:;161:U:0:`,
	"N418": `N418;T412;R417,416,415,413,414,0;0:U:0:;1:D:0:0;2:M:0:1;3:D:0:2;4:M:0:3;5:D:0:4;6:M:0:5;7:D:0:4;8:M:0:7;9:D:0:8;10:M:0:9;11:D:0:8;12:D:0:8;13:D:0:8;14:M:0:13;15:D:0:14;16:M:0:15;17:D:0:16;18:M:0:17;19:M:0:12;20:D:0:16;21:M:0:20;22:D:0:21;23:M:0:22;24:D:0:21;25:M:0:24;26:D:0:23,25;27:M:0:26;28:D:0:27;29:M:0:28;30:D:0:29;31:M:0:30;32:D:0:31;33:M:0:32;34:D:0:33;35:M:0:34;36:D:0:6,10,35;37:D:0:6,10,35;38:D:0:6,10,35;39:M:0:37;40:D:0:39;41:M:0:40;42:D:0:41;43:M:0:42;44:D:0:43;45:M:0:44;46:M:0:36;47:M:0:38;48:D:0:11,18,45,47;49:M:0:48;50:D:0:49;51:M:0:50;52:D:0:51;53:D:0:51;54:M:0:53;55:M:0:52;56:D:0:55;57:M:0:56;58:D:0:54,55;59:D:0:54,55;60:M:0:59;61:D:0:60;62:M:0:61;63:D:0:62;64:M:0:63;65:D:0:64;66:M:0:65;67:D:0:64;68:D:0:66;69:M:0:68;70:D:0:46,69;71:M:0:70;72:M:0:58;73:D:0:19,46,69;74:M:0:73;75:D:0:72,74;76:M:0:75;77:D:0:76;78:M:0:77;79:D:0:76;80:D:0:76;81:M:0:79;82:M:0:80;83:D:0:76;84:M:0:83;85:D:0:81,84;86:M:0:85;87:D:0:86;88:M:0:87;89:M:0:67;90:D:0:88;91:M:0:90;92:D:0:57,78,82,89,91;93:M:0:92;94:D:0:93;95:M:0:94;96:D:0:95;97:M:0:96;98:D:0:95;99:M:0:98;100:D:0:99;101:D:0:71,97,100;102:M:0:101;103:D:0:102;104:D:0:103;105:D:0:104;106:D:0:105;107:D:0:106;108:D:0:107;109:D:0:108,414;110:D:0:109;111:D:0:110;112:D:0:111,413;113:D:0:112;114:D:0:112;115:D:0:114;116:D:0:115;117:D:0:115;118:D:0:113,116,117;119:D:0:118;120:D:0:119;121:D:0:120,415,416,417;122:D:0:121;123:D:0:122;124:D:0:123;125:D:0:124;126:D:0:125;127:D:0:125;128:D:0:127;129:D:0:126,128;130:D:0:126,128;131:D:0:129;132:D:0:129,130;133:D:0:132;134:D:0:133;135:D:0:134;136:D:0:134;137:D:0:131,135,136;138:D:0:137;139:D:0:137;140:D:0:138,139;141:D:0:140;142:D:0:141;143:D:0:141;144:D:0:142,143;145:D:0:144;146:D:0:145;147:D:0:146;148:D:0:147;149:D:0:148;150:D:0:149;151:D:0:150;152:D:0:150;153:D:0:152;154:D:0:152;155:D:0:154;156:D:0:155;157:D:0:153,156;158:D:0:151,157;159:D:0:158;160:D:0:158;161:D:0:160;162:D:0:161;163:D:0:159,162;164:D:0:163;165:D:0:164;166:D:0:164;167:D:0:164;168:D:0:167;169:D:0:165,166,168;170:D:0:169;171:D:0:170;172:D:0:171;173:D:0:172;174:D:0:173;175:D:0:174;176:D:0:174;177:D:0:176;178:D:0:177;179:D:0:178;180:D:0:178;181:D:0:180;182:D:0:179,181;183:D:0:175,182;184:D:0:183;185:D:0:184;186:D:0:185;187:D:0:186;188:D:0:186;189:D:0:186;190:D:0:186;191:D:0:190;192:D:0:191;193:D:0:189,192;194:D:0:193;195:D:0:194;196:D:0:188,195;197:D:0:196;198:D:0:196;199:D:0:197,198;200:D:0:199;201:D:0:200;202:D:0:201;203:D:0:187,202;204:D:0:203;205:D:0:203;206:D:0:204,205;207:D:0:204,205;208:D:0:207;209:D:0:208;210:D:0:209;211:D:0:210;212:D:0:211;213:D:0:211;214:D:0:206,213;215:D:0:206,213;216:D:0:215;217:D:0:216;218:D:0:217;219:D:0:218;220:D:0:212,214,219;221:D:0:220;222:D:0:220;223:D:0:221,222;224:D:0:223;225:D:0:224;226:D:0:225;227:D:0:225;228:D:0:226,227;229:D:0:228;230:D:0:229;231:D:0:230;232:D:0:231;233:D:0:232;234:D:0:232;235:D:0:233,234;236:D:0:235;237:D:0:235;238:D:0:236,237;239:D:0:238;240:D:0:239;241:D:0:240;242:D:0:241;243:D:0:242;244:D:0:243;245:D:0:244;246:D:0:245;247:D:0:246;248:D:0:247;249:D:0:248;250:D:0:249;251:N:0:250;252:D:0:251;253:B:0:252;254:N:0:250,253;255:D:0:254;256:B:0:255;257:N:0:252,253;258:D:0:257;259:B:0:258;260:N:0:252,253,256;261:D:0:260;262:B:0:261;263:D:0:259,262;264:M:0:263;265:D:0:264;266:M:0:265;267:D:0:266;268:M:0:267;269:D:0:268;270:M:0:269;271:D:0:270;272:M:0:271;273:D:0:272;274:M:0:273;275:D:0:272;276:M:0:275;277:D:0:276;278:M:0:277;279:D:0:276;280:M:0:279;281:D:0:274,280;282:M:0:281;283:D:0:282;284:M:0:283;285:D:0:284;286:M:0:285;287:D:0:278,286;288:M:0:287;289:D:0:278,286;290:M:0:289;291:D:0:290;292:M:0:291;293:D:0:292;294:M:0:293;295:D:0:288,294;296:M:0:295;297:D:0:296;298:M:0:297;299:D:0:296;300:M:0:299;301:D:0:298,300;302:M:0:301;303:D:0:302;304:M:0:303;305:D:0:304;306:M:0:305;307:D:0:306;308:M:0:307;309:D:0:308;310:M:0:309;311:D:0:310;312:M:0:311;313:D:0:312;314:M:0:313;315:D:0:314;316:M:0:315;317:D:0:316;318:M:0:317;319:D:0:316;320:M:0:319;321:D:0:318,320;322:M:0:321;323:D:0:322;324:M:0:323;325:D:0:324;326:M:0:325;327:D:0:326;328:M:0:327;329:D:0:328;330:M:0:329;331:D:0:330;332:M:0:331;333:D:0:332;334:M:0:333;335:D:0:334;336:M:0:335;337:D:0:336;338:M:0:337;339:D:0:338;340:D:0:338;341:M:0:340;342:D:0:341;343:D:0:342;344:M:0:343;345:D:0:342;346:M:0:339;347:M:0:345;348:D:0:346,347;349:M:0:348;350:D:0:346,347;351:M:0:350;352:D:0:351;353:M:0:352;354:D:0:353;355:M:0:354;356:D:0:344,349,355;357:M:0:356;358:D:0:357;359:M:0:358;360:D:0:359;361:M:0:360;362:D:0:361;363:M:0:362;364:D:0:363;365:D:0:364;366:D:0:365;367:D:0:366;368:D:0:367;369:D:0:368;370:D:0:369;371:D:0:370;372:D:0:371;373:D:0:372;374:D:0:373;375:D:0:374;376:D:0:375;377:D:0:376;378:D:0:377;379:D:0:378;380:D:0:379;381:D:0:380;382:D:0:381;383:D:0:382;384:D:0:383;385:D:0:384;386:D:0:385;387:D:0:386;388:D:0:387;389:D:0:388;390:D:0:389;391:D:0:390;392:D:0:391;393:D:0:392;394:D:0:393;395:D:0:394;396:D:0:395;397:D:0:396;398:D:0:397;399:D:0:398;400:D:0:399;401:D:0:400;402:D:0:401;403:D:0:402;404:D:0:403;405:D:0:404;406:D:0:405;407:D:0:406;408:D:0:407;409:D:0:408;410:D:0:409;411:D:0:410;412:D:0:411;413:U:0:;414:U:0:;415:U:0:;416:U:0:;417:U:0:`,
}

func TestDecodeDAG(t *testing.T) {
	for name, encoded := range benchDAGs {
		dag := decodeDAG(encoded)
		tips := dag.Tips()
		roots := dag.Roots()
		t.Logf("%s: %d nodes, %d tips, %d roots", name, len(dag.byOffset), len(tips), len(roots))
		if len(dag.byOffset) == 0 {
			t.Fatalf("%s: decoded 0 nodes", name)
		}
	}
}

func BenchmarkResolveFull(b *testing.B) {
	for name, encoded := range benchDAGs {
		dag := decodeDAG(encoded)
		b.Run(fmt.Sprintf("%s_%dnodes", name, len(dag.byOffset)), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				// Rebuild DAG each iteration since resolveFull may be destructive
				d := decodeDAG(encoded)
				resolveFull(d)
			}
		})
	}
}

func BenchmarkBuildDAG(b *testing.B) {
	for name, encoded := range benchDAGs {
		// Pre-parse nodes
		dag := decodeDAG(encoded)
		nodes := make([]*DAGNode, 0, len(dag.byOffset))
		for _, n := range dag.byOffset {
			nodes = append(nodes, n)
		}
		b.Run(fmt.Sprintf("%s_%dnodes", name, len(nodes)), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				BuildDAG(nodes)
			}
		})
	}
}
