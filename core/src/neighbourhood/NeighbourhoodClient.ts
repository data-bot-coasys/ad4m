import { ApolloClient, gql, FetchResult } from "@apollo/client/core"
import { Address } from "../Address"
import { DID } from "../DID"
import { OnlineAgent, TelepresenceSignalCallback } from "../language/Language"
import { Perspective, PerspectiveUnsignedInput } from "../perspectives/Perspective"
import { PerspectiveHandle } from "../perspectives/PerspectiveHandle"
import unwrapApolloResult from "../unwrapApolloResult"
import { NeighbourhoodProxy } from "./NeighbourhoodProxy"

export class NeighbourhoodClient {
    #apolloClient: ApolloClient<any>
    #signalHandlers: Map<string, TelepresenceSignalCallback[]> = new Map()

    constructor(client: ApolloClient<any>) {
        this.#apolloClient = client
    }

    async publishFromPerspective(
        perspectiveUUID: string,
        linkLanguage: Address,
        meta: Perspective
    ): Promise<string> {
        const { neighbourhoodPublishFromPerspective } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodPublishFromPerspective(
                $linkLanguage: String!,
                $meta: PerspectiveInput!,
                $perspectiveUUID: String!
            ) {
                neighbourhoodPublishFromPerspective(
                    linkLanguage: $linkLanguage,
                    meta: $meta,
                    perspectiveUUID: $perspectiveUUID
                )
            }`,
            variables: { perspectiveUUID, linkLanguage, meta: meta}
        }))
        return neighbourhoodPublishFromPerspective
    }

    async joinFromUrl(url: string): Promise<PerspectiveHandle> {
        const { neighbourhoodJoinFromUrl } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodJoinFromUrl($url: String!) {
                neighbourhoodJoinFromUrl(url: $url) {
                    uuid
                    name
                    sharedUrl
                    state
                    neighbourhood {
                        data {
                            linkLanguage
                            meta {
                                links
                                    {
                                        author
                                        timestamp
                                        data { source, predicate, target }
                                        proof { valid, invalid, signature, key }
                                    }
                            }
                        }
                        author
                    }
                }
            }`,
            variables: { url }
        }))
        return neighbourhoodJoinFromUrl
    }

    async otherAgents(perspectiveUUID: string): Promise<DID[]> {
        const { neighbourhoodOtherAgents } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query neighbourhoodOtherAgents($perspectiveUUID: String!) {
                neighbourhoodOtherAgents(perspectiveUUID: $perspectiveUUID)
            }`,
            variables: { perspectiveUUID }
        }))
        return neighbourhoodOtherAgents
    }

    async hasTelepresenceAdapter(perspectiveUUID: string): Promise<boolean> {
        const { neighbourhoodHasTelepresenceAdapter } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query neighbourhoodHasTelepresenceAdapter($perspectiveUUID: String!) {
                neighbourhoodHasTelepresenceAdapter(perspectiveUUID: $perspectiveUUID)
            }`,
            variables: { perspectiveUUID }
        }))
        return neighbourhoodHasTelepresenceAdapter
    }

    async onlineAgents(perspectiveUUID: string): Promise<OnlineAgent[]> {
        const { neighbourhoodOnlineAgents } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query neighbourhoodOnlineAgents($perspectiveUUID: String!) {
                neighbourhoodOnlineAgents(perspectiveUUID: $perspectiveUUID) {
                    did
                    status {
                        author
                        timestamp
                        data {
                            links {
                                author
                                timestamp
                                data { source, predicate, target }
                                proof { valid, invalid, signature, key }
                            }
                        }
                        proof { valid, invalid, signature, key }
                    }
                }
            }`,
            variables: { perspectiveUUID }
        }))
        return neighbourhoodOnlineAgents
    }

    async setOnlineStatus(perspectiveUUID: string, status: Perspective): Promise<boolean> {
        const { neighbourhoodSetOnlineStatus } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodSetOnlineStatus(
                $perspectiveUUID: String!,
                $status: PerspectiveInput!
            ) {
                neighbourhoodSetOnlineStatus(
                    perspectiveUUID: $perspectiveUUID,
                    status: $status
                )
            }`,
            variables: { perspectiveUUID, status }
        }))

        return neighbourhoodSetOnlineStatus
    }

    async setOnlineStatusU(perspectiveUUID: string, status: PerspectiveUnsignedInput): Promise<boolean> {
        const { neighbourhoodSetOnlineStatusU } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodSetOnlineStatusU(
                $perspectiveUUID: String!,
                $status: PerspectiveUnsignedInput!
            ) {
                neighbourhoodSetOnlineStatusU(
                    perspectiveUUID: $perspectiveUUID,
                    status: $status
                )
            }`,
            variables: { perspectiveUUID, status }
        }))

        return neighbourhoodSetOnlineStatusU
    }

    async sendSignal(perspectiveUUID: string, remoteAgentDid: string, payload: Perspective): Promise<boolean> {
        const { neighbourhoodSendSignal } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodSendSignal(
                $perspectiveUUID: String!,
                $remoteAgentDid: String!,
                $payload: PerspectiveInput!
            ) {
                neighbourhoodSendSignal(
                    perspectiveUUID: $perspectiveUUID,
                    remoteAgentDid: $remoteAgentDid,
                    payload: $payload
                )
            }`,
            variables: { perspectiveUUID, remoteAgentDid, payload }
        }))

        return neighbourhoodSendSignal
    }

    async sendSignalU(perspectiveUUID: string, remoteAgentDid: string, payload: PerspectiveUnsignedInput): Promise<boolean> {
        const { neighbourhoodSendSignalU } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodSendSignalU(
                $perspectiveUUID: String!,
                $remoteAgentDid: String!,
                $payload: PerspectiveUnsignedInput!
            ) {
                neighbourhoodSendSignalU(
                    perspectiveUUID: $perspectiveUUID,
                    remoteAgentDid: $remoteAgentDid,
                    payload: $payload
                )
            }`,
            variables: { perspectiveUUID, remoteAgentDid, payload }
        }))

        return neighbourhoodSendSignalU
    }

    async sendBroadcast(perspectiveUUID: string, payload: Perspective, loopback: boolean = false): Promise<boolean> {
        const { neighbourhoodSendBroadcast } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodSendBroadcast(
                $perspectiveUUID: String!,
                $payload: PerspectiveInput!,
                $loopback: Boolean
            ) {
                neighbourhoodSendBroadcast(
                    perspectiveUUID: $perspectiveUUID,
                    payload: $payload,
                    loopback: $loopback
                )
            }`,
            variables: { perspectiveUUID, payload, loopback }
        }))

        return neighbourhoodSendBroadcast
    }

    async sendBroadcastU(perspectiveUUID: string, payload: PerspectiveUnsignedInput, loopback: boolean = false): Promise<boolean> {
        const { neighbourhoodSendBroadcastU } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation neighbourhoodSendBroadcastU(
                $perspectiveUUID: String!,
                $payload: PerspectiveUnsignedInput!,
                $loopback: Boolean
            ) {
                neighbourhoodSendBroadcastU(
                    perspectiveUUID: $perspectiveUUID,
                    payload: $payload,
                    loopback: $loopback
                )
            }`,
            variables: { perspectiveUUID, payload, loopback }
        }))

        return neighbourhoodSendBroadcastU
    }

    dispatchSignal(perspectiveUUID:string, signal: any) {
        const handlers = this.#signalHandlers.get(perspectiveUUID)
        if (handlers) {
            for (const handler of handlers) {
                try {
                    handler(signal)
                } catch(e) {
                    console.error("Error in signal handler:", e)
                }
            }
        }
    }

    async subscribeToSignals(perspectiveUUID: string): Promise<void> {
        const that = this
        this.#apolloClient.subscribe({
            query: gql`subscription neighbourhoodSignal($perspectiveUUID: String!) {
                neighbourhoodSignal(perspectiveUUID: $perspectiveUUID) {
                    author
                    timestamp
                    data {
                        links
                            {
                                author
                                timestamp
                                data { source, predicate, target }
                                proof { valid, invalid, signature, key }
                            }
                    }
                    proof { valid, invalid, signature, key }
                }
            }`,
            variables: { perspectiveUUID }
        }).subscribe({
            next: (result: FetchResult<any>) => {
                try {
                    const { neighbourhoodSignal } = unwrapApolloResult(result)
                    that.dispatchSignal(perspectiveUUID, neighbourhoodSignal)
                } catch(e) {
                    console.error("Error in signal subscription:", e)
                }
            },
            error: (err: any) => {
                console.error("Signal subscription error for perspective", perspectiveUUID, err)
            }
        })
    }

    async addSignalHandler(perspectiveUUID: string, handler: TelepresenceSignalCallback): Promise<void> {
        let handlersForPerspective = this.#signalHandlers.get(perspectiveUUID)
        if (!handlersForPerspective) {
            handlersForPerspective = []
            this.#signalHandlers.set(perspectiveUUID, handlersForPerspective)
            // Push handler BEFORE subscribing so it's available when signals arrive
            handlersForPerspective.push(handler)
            await this.subscribeToSignals(perspectiveUUID)
        } else {
            handlersForPerspective.push(handler)
        }
    }

    removeSignalHandler(perspectiveUUID: string, handler: TelepresenceSignalCallback): void {
        const handlersForPerspective = this.#signalHandlers.get(perspectiveUUID)
        if (handlersForPerspective) {
            const index = handlersForPerspective.indexOf(handler)
            if (index > -1) {
                handlersForPerspective.splice(index, 1)
            }
        }
    }

    // ---- SFU API ----

    async sfuStartRoom(neighbourhoodUrl: string, roomId: string): Promise<SfuRoom> {
        const { sfuStartRoom } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation sfuStartRoom($neighbourhoodUrl: String!, $roomId: String!) {
                sfuStartRoom(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId) {
                    neighbourhoodUrl
                    roomName
                    participantCount
                    participants { agentDid hasAudio hasVideo isActiveSpeaker }
                }
            }`,
            variables: { neighbourhoodUrl, roomId }
        }))
        return sfuStartRoom
    }

    async sfuStopRoom(neighbourhoodUrl: string, roomId: string): Promise<boolean> {
        const { sfuStopRoom } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation sfuStopRoom($neighbourhoodUrl: String!, $roomId: String!) {
                sfuStopRoom(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId)
            }`,
            variables: { neighbourhoodUrl, roomId }
        }))
        return sfuStopRoom
    }

    async callJoin(neighbourhoodUrl: string, roomId: string, sdpOffer: string): Promise<CallSession> {
        const { callJoin } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation callJoin($neighbourhoodUrl: String!, $roomId: String!, $sdpOffer: String!) {
                callJoin(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId, sdpOffer: $sdpOffer) {
                    roomName
                    neighbourhoodUrl
                    participantId
                    sdpAnswer
                    redirectTo
                    streamMapping
                }
            }`,
            variables: { neighbourhoodUrl, roomId, sdpOffer }
        }))
        return callJoin
    }


    async callRenegotiate(neighbourhoodUrl: string, roomId: string, sdpOffer: string): Promise<CallSession> {
        const { callRenegotiate } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation callRenegotiate($neighbourhoodUrl: String!, $roomId: String!, $sdpOffer: String!) {
                callRenegotiate(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId, sdpOffer: $sdpOffer) {
                    roomName
                    neighbourhoodUrl
                    participantId
                    sdpAnswer
                    redirectTo
                    streamMapping
                }
            }`,
            variables: { neighbourhoodUrl, roomId, sdpOffer }
        }))
        return callRenegotiate
    }

    async callLeave(neighbourhoodUrl: string, roomId: string): Promise<boolean> {
        const { callLeave } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation callLeave($neighbourhoodUrl: String!, $roomId: String!) {
                callLeave(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId)
            }`,
            variables: { neighbourhoodUrl, roomId }
        }))
        return callLeave
    }

    async callSetQualityPreference(neighbourhoodUrl: string, roomId: string, preference: string): Promise<boolean> {
        const { callSetQualityPreference } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation callSetQualityPreference($neighbourhoodUrl: String!, $roomId: String!, $preference: String!) {
                callSetQualityPreference(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId, preference: $preference)
            }`,
            variables: { neighbourhoodUrl, roomId, preference }
        }))
        return callSetQualityPreference
    }

    async sfuRooms(): Promise<SfuRoom[]> {
        const { sfuRooms } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query sfuRooms {
                sfuRooms {
                    neighbourhoodUrl
                    roomName
                    participantCount
                    participants { agentDid hasAudio hasVideo isActiveSpeaker }
                }
            }`
        }))
        return sfuRooms
    }

    async sfuPeersForNeighbourhood(neighbourhoodUrl: string): Promise<string[]> {
        const { sfuPeersForNeighbourhood } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query sfuPeersForNeighbourhood($neighbourhoodUrl: String!) {
                sfuPeersForNeighbourhood(neighbourhoodUrl: $neighbourhoodUrl)
            }`,
            variables: { neighbourhoodUrl }
        }))
        return sfuPeersForNeighbourhood
    }

    async sfuAnnounce(neighbourhoodUrl: string, roomId: string): Promise<boolean> {
        const { sfuAnnounce } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation sfuAnnounce($neighbourhoodUrl: String!, $roomId: String!) {
                sfuAnnounce(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId)
            }`,
            variables: { neighbourhoodUrl, roomId }
        }))
        return sfuAnnounce
    }

    async sfuNodesForRoom(neighbourhoodUrl: string, roomId: string): Promise<SfuNode[]> {
        const { sfuNodesForRoom } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query sfuNodesForRoom($neighbourhoodUrl: String!, $roomId: String!) {
                sfuNodesForRoom(neighbourhoodUrl: $neighbourhoodUrl, roomId: $roomId) {
                    did
                    participantCount
                    capacityHint
                }
            }`,
            variables: { neighbourhoodUrl, roomId }
        }))
        return sfuNodesForRoom
    }

    async sfuPeerForNeighbourhood(neighbourhoodUrl: string): Promise<string | null> {
        const { sfuPeerForNeighbourhood } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query sfuPeerForNeighbourhood($neighbourhoodUrl: String!) {
                sfuPeerForNeighbourhood(neighbourhoodUrl: $neighbourhoodUrl)
            }`,
            variables: { neighbourhoodUrl }
        }))
        return sfuPeerForNeighbourhood
    }

    async sfuConfig(neighbourhoodUrl: string): Promise<SfuConfig> {
        const { sfuConfig } = unwrapApolloResult(await this.#apolloClient.query({
            query: gql`query sfuConfig($neighbourhoodUrl: String!) {
                sfuConfig(neighbourhoodUrl: $neighbourhoodUrl) {
                    mode
                    designatedPeer
                    sfuPeers
                    fallback
                    maxMeshParticipants
                    maxParticipantsPerNode
                }
            }`,
            variables: { neighbourhoodUrl }
        }))
        return sfuConfig
    }

    async sfuSetConfig(neighbourhoodUrl: string, config: Partial<SfuConfig>): Promise<boolean> {
        const { sfuSetConfig } = unwrapApolloResult(await this.#apolloClient.mutate({
            mutation: gql`mutation sfuSetConfig(
                $neighbourhoodUrl: String!,
                $mode: String!,
                $designatedPeer: String,
                $fallback: String,
                $maxMeshParticipants: Int
            ) {
                sfuSetConfig(
                    neighbourhoodUrl: $neighbourhoodUrl,
                    mode: $mode,
                    designatedPeer: $designatedPeer,
                    fallback: $fallback,
                    maxMeshParticipants: $maxMeshParticipants
                )
            }`,
            variables: {
                neighbourhoodUrl,
                mode: config.mode || "mesh",
                designatedPeer: config.designatedPeer,
                fallback: config.fallback,
                maxMeshParticipants: config.maxMeshParticipants,
            }
        }))
        return sfuSetConfig
    }
}

// SFU types
export interface SfuRoom {
    neighbourhoodUrl: string
    roomName: string
    participantCount: number
    participants: SfuParticipant[]
}

export interface SfuParticipant {
    agentDid: string
    hasAudio: boolean
    hasVideo: boolean
    isActiveSpeaker: boolean
}

export interface CallSession {
    roomName: string
    neighbourhoodUrl: string
    participantId: string
    sdpAnswer: string
    redirectTo: string | null
    streamMapping: string[]
}

export interface SfuConfig {
    mode: "gateway" | "designated" | "mesh" | "cascaded"
    designatedPeer: string | null
    sfuPeers: string[]
    fallback: string
    maxMeshParticipants: number
    maxParticipantsPerNode: number | null
}

export interface SfuNode {
    did: string
    participantCount: number
    capacityHint: number
}
