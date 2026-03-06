import { DID } from "../DID";
import { OnlineAgent } from "../language/Language";
import { Perspective, PerspectiveExpression, PerspectiveUnsignedInput } from "../perspectives/Perspective";
import { NeighbourhoodClient, SfuRoom, CallSession, SfuConfig, SfuNode } from "./NeighbourhoodClient";

export class NeighbourhoodProxy {
    #client: NeighbourhoodClient
    #pID: string

    constructor(client: NeighbourhoodClient, pID: string) {
        this.#client = client
        this.#pID = pID
    }

    async otherAgents(): Promise<DID[]> {
        return await this.#client.otherAgents(this.#pID)
    }

    async hasTelepresenceAdapter(): Promise<boolean> {
        return await this.#client.hasTelepresenceAdapter(this.#pID)
    }

    async onlineAgents(): Promise<OnlineAgent[]> {
        return await this.#client.onlineAgents(this.#pID)
    }

    async setOnlineStatus(status: Perspective): Promise<boolean> {
        return await this.#client.setOnlineStatus(this.#pID, status)
    }

    async setOnlineStatusU(status: PerspectiveUnsignedInput): Promise<boolean> {
        return await this.#client.setOnlineStatusU(this.#pID, status)
    }

    async sendSignal(remoteAgentDid: string, payload: Perspective): Promise<boolean> {
        return await this.#client.sendSignal(this.#pID, remoteAgentDid, payload)
    }

    async sendSignalU(remoteAgentDid: string, payload: PerspectiveUnsignedInput): Promise<boolean> {
        return await this.#client.sendSignalU(this.#pID, remoteAgentDid, payload)
    }

    async sendBroadcast(payload: Perspective, loopback: boolean = false): Promise<boolean> {
        return await this.#client.sendBroadcast(this.#pID, payload, loopback)
    }

    async sendBroadcastU(payload: PerspectiveUnsignedInput, loopback: boolean = false): Promise<boolean> {
        return await this.#client.sendBroadcastU(this.#pID, payload, loopback)
    }

    async addSignalHandler(handler: (payload: PerspectiveExpression) => void): Promise<void> {
        await this.#client.addSignalHandler(this.#pID, handler)
    }

    removeSignalHandler(handler: (payload: PerspectiveExpression) => void) {
        this.#client.removeSignalHandler(this.#pID, handler)
    }

    // ---- SFU API ----

    async sfuStartRoom(roomId: string): Promise<SfuRoom> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuStartRoom(url, roomId)
    }

    async sfuStopRoom(roomId: string): Promise<boolean> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuStopRoom(url, roomId)
    }

    async callJoin(roomId: string, sdpOffer: string): Promise<CallSession> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.callJoin(url, roomId, sdpOffer)
    }

    async callLeave(roomId: string): Promise<boolean> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.callLeave(url, roomId)
    }

    async callSetQualityPreference(roomId: string, preference: string): Promise<boolean> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.callSetQualityPreference(url, roomId, preference)
    }

    async sfuPeer(): Promise<string | null> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuPeerForNeighbourhood(url)
    }

    async sfuPeers(): Promise<string[]> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuPeersForNeighbourhood(url)
    }

    async sfuAnnounce(roomId: string): Promise<boolean> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuAnnounce(url, roomId)
    }

    async sfuNodesForRoom(roomId: string): Promise<SfuNode[]> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuNodesForRoom(url, roomId)
    }

    async sfuConfig(): Promise<SfuConfig> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuConfig(url)
    }

    async sfuSetConfig(config: Partial<SfuConfig>): Promise<boolean> {
        const url = await this.#getNeighbourhoodUrl()
        return await this.#client.sfuSetConfig(url, config)
    }

    // The neighbourhood URL is needed for SFU API calls.
    // We use the perspective UUID to identify the neighbourhood, but the SFU service
    // uses the neighbourhood URL as its key. This helper resolves it.
    async #getNeighbourhoodUrl(): Promise<string> {
        // The perspective UUID IS the neighbourhood URL in the current implementation
        return this.#pID
    }
}
