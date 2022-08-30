import {Readable} from "stream";
import StreamObject from "stream-json/streamers/StreamObject";
import {stream} from "event-iterator";
import {
    convertMarketDeal,
    createStatement,
    dropStatement,
    epochToTimestamp, insertStatement,
    processDeals,
    readMarketDeals
} from "./index";
import * as index from './index';
import createSpyObj = jasmine.createSpyObj;

describe('index', () => {
    const expectedDealRow = [
        9689670,
        'baga6ea4seaqn3jt6lrunp6ucihyxptqx6aemoxddzsa4eyocmydfpxbrwllqwna',
        17179869184n,
        true,
        'f01860352',
        'f01206408',
        'QmagcXGsTErQ8J8KxvkkuybYDK47S4BKu67uqYPUgGgL1S',
        1661998500,
        1707790410,
        0n,
        3350549236814358n,
        0n, 0, 0, 0
    ];
    const expectedDealRow2 = [
        9686908,
        'baga6ea4seaqauc2ydwxtamwtij6xwe7ewoxmxprdoqn4zjnuknz77v7ijewxchq',
        34359738368n,
        true,
        'f01850099',
        'f01895913',
        'mAXCg5AIgrlxdhtWlQYd+xRf20UUMrzw+Gn9F8LogoloEJ0xWvBM',
        1662315360,
        1708539360,
        0n,
        6785282953422315n,
        0n, 0, 0, 0
    ];
    describe('processDeals', () => {
        it('should dump market deals to database and remove old ones', async () => {
            const client = createSpyObj('postgres', ['connect', 'query', 'end']);
            const url = 'https://market-deal-importer.s3.us-west-2.amazonaws.com/test.json';
            await index.processDeals(url, client);
            expect(client.connect).toHaveBeenCalled();
            expect(client.query).toHaveBeenCalledTimes(12);
            expect(client.query).toHaveBeenCalledWith(dropStatement);
            expect(client.query).toHaveBeenCalledWith(createStatement);
            expect(client.query).toHaveBeenCalledWith(jasmine.objectContaining({
                text: insertStatement,
                values: expectedDealRow2
            }));
            expect(client.query).toHaveBeenCalledWith("BEGIN");
            expect(client.query).toHaveBeenCalledWith("DROP TABLE IF EXISTS current_state");
            expect(client.query).toHaveBeenCalledWith("ALTER TABLE current_state_new RENAME TO current_state");
            expect(client.query).toHaveBeenCalledWith("COMMIT");
        })
    })
    describe('epochToTimestamp', () => {
        it('converts epoch to timestamp', () => {
            expect(epochToTimestamp(0)).toBe(1598306400);
            expect(epochToTimestamp(1)).toBe(1598306430);
            expect(epochToTimestamp(-1)).toBe(0);
        })
    })
    describe('convertMarketDeal', () => {
        it('should be able to convert market deal to deal row', () => {
            const deal = {
                key: '9689670',
                value: {
                    "Proposal": {
                        "PieceCID": {"/": "baga6ea4seaqn3jt6lrunp6ucihyxptqx6aemoxddzsa4eyocmydfpxbrwllqwna"},
                        "PieceSize": 17179869184,
                        "VerifiedDeal": true,
                        "Client": "f01860352",
                        "Provider": "f01206408",
                        "Label": "QmagcXGsTErQ8J8KxvkkuybYDK47S4BKu67uqYPUgGgL1S",
                        "StartEpoch": 2123070,
                        "EndEpoch": 3649467,
                        "StoragePricePerEpoch": "0",
                        "ProviderCollateral": "3350549236814358",
                        "ClientCollateral": "0"
                    }, "State": {"SectorStartEpoch": -1, "LastUpdatedEpoch": -1, "SlashEpoch": -1}
                }
            }
            const row = convertMarketDeal(deal);
            expect(row).toEqual(expectedDealRow);
        })
    })
    describe('readMarketDeals', () => {
        it('regular json string should return async iterator', async () => {
            const s = new Readable();
            s.push('{"a":1, "b": 2}');
            s.push(null)
            const p = s.pipe(StreamObject.withParser());
            const result = []
            for await(const a of stream.call(p)) {
                result.push(a);
            }
            expect<any>(result).toEqual([{key: 'a', value: 1}, {key: 'b', value: 2}]);
        })
        it('should parse json stream from test json', async () => {
            const url = 'https://market-deal-importer.s3.us-west-2.amazonaws.com/test.json';
            const result = []
            for await(const a of await readMarketDeals(url)) {
                result.push(a);
            }
            expect(result.length).toBe(6);
            expect(result[0].key).toBe('9686908');
            expect(result[0].value).toEqual({
                Proposal: {
                    PieceCID: {
                        '/': 'baga6ea4seaqauc2ydwxtamwtij6xwe7ewoxmxprdoqn4zjnuknz77v7ijewxchq'
                    },
                    PieceSize: 34359738368,
                    VerifiedDeal: true,
                    Client: 'f01850099',
                    Provider: 'f01895913',
                    Label: 'mAXCg5AIgrlxdhtWlQYd+xRf20UUMrzw+Gn9F8LogoloEJ0xWvBM',
                    StartEpoch: 2133632,
                    EndEpoch: 3674432,
                    StoragePricePerEpoch: '0',
                    ProviderCollateral: '6785282953422315',
                    ClientCollateral: '0'
                }, State: {SectorStartEpoch: -1, LastUpdatedEpoch: -1, SlashEpoch: -1}
            });
        })
    })
})
