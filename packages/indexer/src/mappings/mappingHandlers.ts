import { ExecuteEvent, Message, Transaction, Block, CreditCollection, EventData, MaterialData, MetadataURI, MediaFile, BinaryFile, ApplicantData, WebReference } from "../types";
import {
  CosmosEvent,
  CosmosBlock,
  CosmosMessage,
  CosmosTransaction,
} from "@subql/types-cosmos";
import fetch from "node-fetch";
import { CreditCollectionProps } from "../types/models/CreditCollection";


export async function handleBlock(block: CosmosBlock): Promise<void> {
  const blockRecord = Block.create({
    id: block.block.id,
    height: BigInt(block.block.header.height),
  });
  await blockRecord.save();
}



export async function handleTransaction(tx: CosmosTransaction): Promise<void> {
  // If you want to index each transaction in Cosmos (Stargaze), you could do that here
  const transactionRecord = Transaction.create({
    id: tx.hash,
    blockHeight: BigInt(tx.block.block.header.height),
    timestamp: tx.block.block.header.time,
  });
  await transactionRecord.save();
}


export async function handleMessage(msg: CosmosMessage): Promise<void> {
  const messageRecord = Message.create({
    id: `${msg.tx.hash}-${msg.idx}`,
    blockHeight: BigInt(msg.block.block.header.height),
    txHash: msg.tx.hash,
  });
  await messageRecord.save();
}

export async function handleEvent(event: CosmosEvent): Promise<void> {
  const eventRecord = ExecuteEvent.create({
    id: `${event.tx.hash}-${event.msg.idx}-${event.idx}`,
    blockHeight: BigInt(event.block.block.header.height),
    txHash: event.tx.hash,
  });

  await eventRecord.save();
}

export async function handleIssueCredits(event: CosmosEvent): Promise<void> {
  let denom = fetchPropertyFromEvent(event, "denom");
  let amount = fetchPropertyFromEvent(event, "amount");
  let projectId = fetchPropertyFromEvent(event, "project_id");
  let issuanceDate = Date.now();

  const metadataUris = fetchPropertyFromEvent(event, "metadata_urls");
  const metadataUrisArray = metadataUris.split(",");

  const res = await fetch("https://bafkreidk2n5ymosfbbvfsv4pmsqiajika7kbzqmtca24bsr34co5ftstde.ipfs.w3s.link");
  const data = await res.json();

  const eventData = findPropById("event_data", data["credit_props"]);
  const eventLocation = findPropById("location", eventData.content);
  const eventAmount = findPropById("amount", eventData.content);
  const eventMagnitude = findPropById("magnitude", eventData.content);
  const eventMaterial = findPropById("material", eventData.content);
  const eventRegistrationDate = findPropById("registration_date", eventData.content);
  const aggregationLocation = findPropById("aggregation_location", data["credit_props"]);
  aggregationX = aggregationLocation.content.x;
  aggregationY = aggregationLocation.content.y;

  const eventData = EventData.create({
  });

  const eventRecord = CreditCollection.create({
    id: `${event.tx.hash}-${event.msg.idx}-${event.idx}`,
    denom: denom,
    projectId: parseInt(projectId),
    activeAmount: BigInt(amount),
    retiredAmount: BigInt(0),
    issuanceDate: issuanceDate.toString(),
    x_aggregation_location: aggregationX,
    y_aggregation_location: aggregationY,
  });
  await eventRecord.save();
}

function findPropById(id: string, creditProps: any[]): any {
  return creditProps.find((prop) => prop.id === id);
}

function fetchPropertyFromEvent(event: CosmosEvent, property: string): string {
  return event.event.attributes.find((attr) => attr.key === property)?.value.replace(/"/g, "");
}

async function fetchMetadataFromIpfs(url: string): Promise<any> {
  const reqUri = "https://" + url.substring(7) + ".ipfs.w3s.link";
  const res = await fetch(reqUri);
  return res.json();
}

async function handleEventData(eventDataJson: any, creditCollectionId: string, eventIndex: string): Promise<void> {
  const eventData = EventData.create({
    id: `${creditCollectionId}-${eventIndex}`,
    latitude: findPropById("location", eventDataJson.content).content.latitude,
    longitude: findPropById("location", eventDataJson.content).content.longitude,
    amount: findPropById("amount", eventDataJson.content).content,
    magnitude: findPropById("magnitude", eventDataJson.content).content,
    registrationDate: findPropById("registration_date", eventDataJson.content).content,
    creditCollectionId: creditCollectionId,
  })
  await eventData.save();
  for (let [i, material] of findPropById("material", eventDataJson.content).content.entries()) {
    await handleMaterialData(material, eventData.id, i);
  }
}

async function handleMaterialData(materialDataJson: any, eventDataId: string, materialIndex: string): Promise<void> {
  const materialData = MaterialData.create({
    id: `${eventDataId}-${materialIndex}`,
    key: materialDataJson.key,
    value: materialDataJson.value,
    eventDataId: eventDataId,
  })
  await materialData.save();
}

async function handleMetadataUris(metadataUris: string[], creditCollectionId: string): Promise<void> {
  for (let [i, url] of metadataUris.entries()) {
    const metadataUri = MetadataURI.create({
      id: `${creditCollectionId}-${i}`,
      url: url,
      creditCollectionId: creditCollectionId,
    })
    await metadataUri.save();
  }
}

async function handleMediaFiles(mediaFiles: any, creditCollectionId: string): Promise<void> {
  for (let [i, mediaFileJson] of mediaFiles.entries()) {
    const mediaFile = MediaFile.create({
      id: `${creditCollectionId}-${i}`,
      url: mediaFileJson.url,
      name: mediaFileJson.name,
      creditCollectionId: creditCollectionId,
    })
    await mediaFile.save();
  }
}

async function handleBinaryFiles(binaryFiles: any, creditCollectionId: string): Promise<void> {
  for (let [i, binaryFileJson] of binaryFiles.entries()) {
    const binaryFile = BinaryFile.create({
      id: `${creditCollectionId}-${i}`,
      url: binaryFileJson.url,
      name: binaryFileJson.name,
      creditCollectionId: creditCollectionId,
    })
    await binaryFile.save();
  }
}

async function handleApplicantData(applicantDataJson: any, creditCollectionId: string): Promise<void> {
  const applicantData = ApplicantData.create({
    id: creditCollectionId,
    name: applicantDataJson.name,
    description: applicantDataJson.description,
    creditCollectionId: creditCollectionId,
  })
  await applicantData.save();
  await handleWebReferences(applicantDataJson.web_refs, applicantData.id);
}

async function handleWebReferences(webReferences: any, applicantId: string): Promise<void> {
  for (let [i, webReferenceJson] of webReferences.entries()) {
    const webReference = WebReference.create({
      id: `${applicantId}-${i}`,
      url: webReferenceJson.url,
      applicantDataId: applicantId,
    })
    await webReference.save();
  }
}