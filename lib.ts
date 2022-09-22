import axios from "axios";
import StreamObject from "stream-json/streamers/StreamObject";
import {stream} from "event-iterator"
import {EventIterator} from "event-iterator/src/event-iterator";
import {Client as PgClient} from 'pg';
// @ts-ignore
import TaskQueue from '@goodware/task-queue';
import JsonRpcClient from "./JsonRpcClient";

export const createStatement = `CREATE TABLE IF NOT EXISTS current_state (
    deal_id INTEGER NOT NULL PRIMARY KEY,
    piece_cid TEXT NOT NULL,
    piece_size BIGINT NOT NULL,
    verified_deal BOOLEAN NOT NULL,
    client TEXT NOT NULL,
    provider TEXT NOT NULL,
    label TEXT NOT NULL,
    start_epoch INTEGER NOT NULL,
    end_epoch INTEGER NOT NULL,
    storage_price_per_epoch BIGINT NOT NULL,
    provider_collateral BIGINT NOT NULL,
    client_collateral BIGINT NOT NULL,
    sector_start_epoch INTEGER NOT NULL,
    last_updated_epoch INTEGER NOT NULL,
    slash_epoch INTEGER NOT NULL
)`;

export const createClientMappingStatement = `CREATE TABLE IF NOT EXISTS client_mapping (
    client TEXT NOT NULL PRIMARY KEY,
    client_address TEXT NOT NULL
)`;

export const getAllClientMappingStatement = `SELECT client, client_address FROM client_mapping`;

export const insertClientMappingStatement = `INSERT INTO client_mapping (client, client_address) VALUES ($1, $2)`;

export const insertStatementBase = `INSERT INTO current_state (
                               deal_id,
                               piece_cid,
                               piece_size,
                               verified_deal,
                               client,
                               provider,
                               label,
                               start_epoch,
                               end_epoch,
                               storage_price_per_epoch,
                               provider_collateral,
                               client_collateral,
                               sector_start_epoch,
                               last_updated_epoch,
                               slash_epoch) VALUES {values}
                            ON CONFLICT (deal_id) DO UPDATE SET sector_start_epoch = EXCLUDED.sector_start_epoch, last_updated_epoch = EXCLUDED.last_updated_epoch, slash_epoch = EXCLUDED.slash_epoch`;

type DealId = number;
type PieceCid = string;
type PieceSize = bigint;
type VerifiedDeal = boolean;
type Client = string;
type Provider = string;
type Label = string;
type StartEpoch = number;
type EndEpoch = number;
type StoragePricePerEpoch = bigint;
type ProviderCollateral = bigint;
type ClientCollateral = bigint;
type SectorStartEpoch = number;
type LastUpdatedEpoch = number;
type SlashEpoch = number;
type DealRow = [
    DealId,
    PieceCid,
    PieceSize,
    VerifiedDeal,
    Client,
    Provider,
    Label,
    StartEpoch,
    EndEpoch,
    StoragePricePerEpoch,
    ProviderCollateral,
    ClientCollateral,
    SectorStartEpoch,
    LastUpdatedEpoch,
    SlashEpoch
]

interface MarketDeal {
    Proposal: {
        PieceCID: {
            '/': string,
        },
        PieceSize: number,
        VerifiedDeal: boolean,
        Client: string,
        Provider: string,
        Label: string,
        StartEpoch: number,
        EndEpoch: number
        StoragePricePerEpoch: string,
        ProviderCollateral: string,
        ClientCollateral: string,
    }
    ,
    State: {
        SectorStartEpoch: number,
        LastUpdatedEpoch: number,
        SlashEpoch: number,
    },
}

export function getInsertStatement(batch: number): string {
    let j = 1;
    let result = '';
    for (let i = 0; i < batch; i++) {
        result += `($${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++})`;
        if (i < batch - 1) {
            result += ', ';
        }
    }
    return insertStatementBase.replace('{values}', result);
}

export async function* readMarketDealsBatch(url: string, batch: number): AsyncIterable<{ key: string, value: MarketDeal }[]> {
    let list = [];
    for await (const deal of await readMarketDeals(url)) {
        list.push(deal);
        if (list.length >= batch) {
            yield [...list];
            list = [];
        }
    }
    if (list.length > 0) {
        yield [...list];
    }
}

export async function readMarketDeals(url: string): Promise<AsyncIterable<{ key: string, value: MarketDeal }>> {
    console.info('Reading market deals from', url);
    const response = await axios.get<NodeJS.ReadableStream>(url, {
        responseType: "stream"
    });
    const p = response.data.pipe(StreamObject.withParser());
    const result = <EventIterator<{ key: string, value: MarketDeal }>>stream.call(p);
    return result;
}

export function epochToTimestamp(epoch: number): number {
    if (epoch === -1) {
        return 0;
    }
    return 1598306400 + epoch * 30;
}

export function convertMarketDeal(deal: {key: string, value: MarketDeal}) : DealRow {
    const { Proposal, State } = deal.value;
    const { PieceCID, PieceSize, VerifiedDeal, Client, Provider, Label, StartEpoch, EndEpoch, StoragePricePerEpoch, ProviderCollateral, ClientCollateral } = Proposal;
    const { SectorStartEpoch, LastUpdatedEpoch, SlashEpoch } = State;
    return [
        parseInt(deal.key),
        PieceCID['/'],
        BigInt(PieceSize),
        VerifiedDeal,
        Client,
        Provider,
        Label,
        StartEpoch,
        EndEpoch,
        BigInt(StoragePricePerEpoch),
        BigInt(ProviderCollateral),
        BigInt(ClientCollateral),
        SectorStartEpoch,
        LastUpdatedEpoch,
        SlashEpoch
    ];
}

const createPieceCidIndex = 'CREATE INDEX IF NOT EXISTS current_state_piece_cid ON current_state (piece_cid)';

const createClientIndex = 'CREATE INDEX IF NOT EXISTS current_state_client ON current_state (client)';

const createProviderIndex = 'CREATE INDEX IF NOT EXISTS current_state_provider ON current_state (provider)';

export async function processDeals(url: string, postgres: PgClient): Promise<void> {
    const queue = new TaskQueue({
        size: parseInt(process.env.QUEUE_SIZE || '8')
    });
    let count = 0;
    let innerCount = 0;
    const batch = parseInt(process.env.BATCH_SIZE || '100');
    const batchInsertStatement = getInsertStatement(batch);
    await postgres.connect();
    try {
        console.info(createStatement);
        await postgres.query(createStatement);
        console.info(createClientMappingStatement);
        await postgres.query(createClientMappingStatement);
        await postgres.query(createPieceCidIndex);
        await postgres.query(createClientIndex);
        await postgres.query(createProviderIndex);
        const clientMappingRows: { client: string, client_address: string }[] = (await postgres.query(getAllClientMappingStatement)).rows;
        const clientMapping = new Map<string, string>();
        for (const row of clientMappingRows) {
            clientMapping.set(row.client, row.client_address);
        }
        const newClients = new Set<string>();

        for await (const marketDeal of await readMarketDealsBatch(url, batch)) {
            for(const deal of marketDeal) {
                const client = deal.value.Proposal.Client;
                if (!clientMapping.has(client)) {
                    newClients.add(client);
                }
            }
            await queue.push(async () => {
                try {
                    if (marketDeal.length === batch) {
                        await postgres.query({
                            name: 'insert-new-deal-batch',
                            text: batchInsertStatement,
                            values: marketDeal.map(convertMarketDeal).flat()
                        });
                    } else {
                        await postgres.query({
                            text: getInsertStatement(marketDeal.length),
                            values: marketDeal.map(convertMarketDeal).flat()
                        });
                    }
                } catch (e) {
                    for (const deal of marketDeal) {
                        console.error(deal);
                    }
                    console.error(e);
                    throw e;
                }
                count += marketDeal.length;
                innerCount += marketDeal.length;
                if (innerCount >= 10000) {
                    innerCount -= 10000;
                    console.log(`Processed ${count} deals`);
                }
            });
        }
        const jsonRpcClient = new JsonRpcClient('https://api.node.glif.io/rpc/v0', 'Filecoin.');
        for (const client of newClients) {
            const result = await jsonRpcClient.call('StateAccountKey', [client, null]);
            await queue.push(async () => {
                if (!result.error && result.result) {
                    const address = result.result;
                    await postgres.query({
                        text: insertClientMappingStatement,
                        values: [client, address]
                    });
                }
            });
        }
        await queue.stop();
    } finally {
        await postgres.end();
    }
    console.log('Total processed', count, 'deals');
}

export async function handler() {
    const url = process.env.INPUT_URL || 'https://market-deal-importer.s3.us-west-2.amazonaws.com/test.json';
    console.log({
        BATCH_SIZE: process.env.BATCH_SIZE,
        QUEUE_SIZE: process.env.QUEUE_SIZE,
        PGHOST: process.env.PGHOST,
        PGPORT: process.env.PGPORT,
        PGUSER: process.env.PGUSER,
        PGDATABASE: process.env.PGDATABASE,
        POLL_CONNECTION_TIMEOUT: process.env.POLL_CONNECTION_TIMEOUT,
        INPUT_URL: process.env.INPUT_URL,
        url: url
    });
    const postgres = new PgClient({
        connectionTimeoutMillis: parseInt(process.env.POLL_CONNECTION_TIMEOUT || '0'),
    });
    await processDeals(url, postgres);
    const response = {
        statusCode: 200
    };
    return response;
}
