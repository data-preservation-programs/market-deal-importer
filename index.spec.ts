import {Readable} from "stream";
import StreamObject from "stream-json/streamers/StreamObject";
import {stream} from "event-iterator";
import {
    convertMarketDeal,
    createStatement,
    dropStatement,
    epochToTimestamp,
    processDeals,
    readMarketDeals, readMarketDealsBatch
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
        2123070,
        3649467,
        0n,
        3350549236814358n,
        0n, -1, -1, -1
    ];
    describe('processDeals', () => {
        it('should dump market deals to database and remove old ones', async () => {
            const client = createSpyObj('postgres', ['connect', 'query', 'end']);
            const url = 'https://market-deal-importer.s3.us-west-2.amazonaws.com/test.json';
            await index.processDeals(url, client);
            expect(client.connect).toHaveBeenCalled();
            expect(client.query).toHaveBeenCalledTimes(7);
            expect(client.query).toHaveBeenCalledWith(dropStatement);
            expect(client.query).toHaveBeenCalledWith(createStatement);
            expect(client.query).toHaveBeenCalledWith("BEGIN");
            expect(client.query).toHaveBeenCalledWith("DROP TABLE IF EXISTS current_state");
            expect(client.query).toHaveBeenCalledWith("ALTER TABLE current_state_new RENAME TO current_state");
            expect(client.query).toHaveBeenCalledWith("COMMIT");
        })
    })
    describe('epochToEpoch', () => {
        it('converts epoch to Epoch', () => {
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
    describe('getInsertStatement', () => {
        it('should return correct statement when batch is 1', () => {
            const statement = index.getInsertStatement(1);
            expect(statement.endsWith("VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)")).toBeTrue();
        })
        it('should return correct statement when batch is 2', () => {
            const statement = index.getInsertStatement(2);
            expect(statement.endsWith("VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15), ($16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)")).toBeTrue();
        })
    })
    describe('readMarketDeals', () => {
        it('should read deals in batch', async () => {
            const url = 'https://market-deal-importer.s3.us-west-2.amazonaws.com/test.json';
            const result = []
            for await(const a of readMarketDealsBatch(url, 4)) {
                result.push(a);
            }
            expect(result.length).toBe(2);
            expect(result[0].length).toBe(4);
            expect(result[1].length).toBe(2);
            expect(result[0][0].key).toBe('9686908');
        })
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
