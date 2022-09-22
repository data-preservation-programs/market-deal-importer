import JsonRpcClient from "./JsonRpcClient";

fdescribe('JsonRpcClient', () => {
  it('should return address for client id', async () => {
    const jsonRpcClient = new JsonRpcClient('https://api.node.glif.io/rpc/v0', 'Filecoin.');
    const result = await jsonRpcClient.call('StateAccountKey', ['f01819389', null]);
    expect(result.result).toEqual('f1ws3n5tuxtyg26lraqkjirz7qon7y7ckju7hhmii')
  })
})
