import {Context} from "aws-lambda";
import {EventBridgeEvent} from "aws-lambda/trigger/eventbridge";
import axios from "axios";
import StreamObject from "stream-json/streamers/StreamObject";
import {stream} from "event-iterator"
import {EventIterator} from "event-iterator/src/event-iterator";
import {Client as PostgresClient} from 'pg';

export const dropStatement = 'DROP TABLE IF EXISTS current_state_new';
export const createStatement = `CREATE TABLE current_state_new (
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
export const insertStatement = `INSERT INTO current_state_new (deal_id,
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
                               slash_epoch)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`;

type DealId = number;
type PieceCid = string;
type PieceSize = bigint;
type VerifiedDeal = boolean;
type Client = string;
type Provider = string;
type Label = string;
type StartTimestamp = number;
type EndTimestamp = number;
type StoragePricePerEpoch = bigint;
type ProviderCollateral = bigint;
type ClientCollateral = bigint;
type SectorStartTimestamp = number;
type LastUpdatedTimestamp = number;
type SlashTimestamp = number;
type DealRow = [
    DealId,
    PieceCid,
    PieceSize,
    VerifiedDeal,
    Client,
    Provider,
    Label,
    StartTimestamp,
    EndTimestamp,
    StoragePricePerEpoch,
    ProviderCollateral,
    ClientCollateral,
    SectorStartTimestamp,
    LastUpdatedTimestamp,
    SlashTimestamp
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

interface InputEvent {
    url: string;
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
        epochToTimestamp(StartEpoch),
        epochToTimestamp(EndEpoch),
        BigInt(StoragePricePerEpoch),
        BigInt(ProviderCollateral),
        BigInt(ClientCollateral),
        epochToTimestamp(SectorStartEpoch),
        epochToTimestamp(LastUpdatedEpoch),
        epochToTimestamp(SlashEpoch),
    ];
}

export async function processDeals(url: string, postgres: PostgresClient): Promise<void> {
    await postgres.connect();
    let count = 0;
    try {
        console.info(dropStatement);
        await postgres.query(dropStatement);
        console.info(createStatement);
        await postgres.query(createStatement);
        console.info(createStatement);

        for await (const marketDeal of await readMarketDeals(url)) {
            await postgres.query({
                name: 'insert-new-deal',
                text: insertStatement,
                values: convertMarketDeal(marketDeal)
            });
            count++;
            if (count % 1000 === 0) {
                console.info(`Processed ${count} deals`);
            }
        }
        console.log('Rename current_state_new to current_state');
        try {
            await postgres.query('BEGIN');
            await postgres.query('DROP TABLE IF EXISTS current_state');
            await postgres.query('ALTER TABLE current_state_new RENAME TO current_state');
        } catch (e) {
            await postgres.query('ROLLBACK')
            throw e;
        } finally {
            await postgres.query('COMMIT');
        }
    } finally {
        await postgres.end();
    }
    console.log('Processed', count, 'deals');
}

export async function handler(event: InputEvent) {
    const url = event.url;
    const postgres = new PostgresClient();
    await processDeals(url, postgres);
    const response = {
        statusCode: 200
    };
    return response;
}
