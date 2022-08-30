import {Context} from "aws-lambda";
import {EventBridgeEvent} from "aws-lambda/trigger/eventbridge";

export async function handler(_event: EventBridgeEvent<any, any>, _context: Context) {
    // TODO implement
    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello from Lambda!'),
    };
    return response;
}
