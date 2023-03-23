import { ExecuteEvent, Message, Transaction, Block, CreditCollection, EventData, MaterialData, MetadataUri, MediaFile, BinaryFile, ApplicantData, WebReference, IssuanceInfo } from "../types";
import {
  CosmosEvent,
  CosmosBlock,
  CosmosMessage,
  CosmosTransaction,
} from "@subql/types-cosmos";
import fetch from "node-fetch";


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
  let creditTypeAbbreviation = fetchPropertyFromEvent(event, "credit_class_abbreviation");

  const metadataUrls = fetchPropertyFromEvent(event, "metadata_uris");
  const metadataUrlsArray = decodeUriArrayFromEvent(metadataUrls);

  const creditCollection = CreditCollection.create({
    id: denom,
    denom: denom,
    projectId: parseInt(projectId),
    activeAmount: BigInt(amount),
    retiredAmount: BigInt(0),
    creditType: creditTypeAbbreviation,
  });
  await creditCollection.save();

  await handleMetadataUris(metadataUrlsArray, creditCollection.id);
  
  for (let [i, metadataUri] of metadataUrlsArray.entries()) {
    const metadata = await fetchMetadataFromIpfs(metadataUri);
    await handleIssuanceInfo(metadata, creditCollection.id, i.toString());
  }

}

function findPropById(id: string, creditProps: any[]): any {
  return creditProps.find((prop) => prop.id === id);
}

function fetchPropertyFromEvent(event: CosmosEvent, property: string): string {
  return event.event.attributes.find((attr) => attr.key === property)?.value.replace(/"/g, "");
}

function decodeUriArrayFromEvent(eventUris: string): string[] {
  eventUris = eventUris.replace(/\[/g, "");
  eventUris = eventUris.replace(/\]/g, "");
  return eventUris.split(",");
}

async function fetchMetadataFromIpfs(url: string): Promise<any> {
  const reqUri = "https://" + url.substring(7) + ".ipfs.w3s.link";
  const res = await fetch(reqUri);
  return res.json();
}

async function handleIssuanceInfo(metadata: any, creditCollectionId: string, issuanceInfoIndex: string): Promise<void> {
  const issuanceInfo = IssuanceInfo.create({
    id: `${creditCollectionId}-${issuanceInfoIndex}`,
    issuanceDate: findPropById("issuance_date", metadata["credit_props"]).content,
    creditType: findPropById("credit_type", metadata["credit_props"]).content,
    // For now, we take only amount from first event
    amount: findPropById("amount", findPropById("event_data", metadata["credit_props"]).content[0]).content,
    aggregationLatitude: findPropById("aggregation_location", metadata["credit_props"]).content.latitude,
    aggregationLongitude: findPropById("aggregation_location", metadata["credit_props"]).content.longitude,
    creditCollectionId: creditCollectionId,
  })
  await issuanceInfo.save();
  const eventData = findPropById("event_data", metadata["credit_props"]);
  for (let [i, event] of eventData.content.entries()) {
    await handleEventData(event, issuanceInfo.id, i.toString());
  }
  const mediaFiles = findPropById("event_media", metadata["credit_props"]);
  await handleMediaFiles(mediaFiles, issuanceInfo.id);
  const binaryFiles = findPropById("event_files", metadata["credit_props"]);
  await handleBinaryFiles(binaryFiles, issuanceInfo.id);
  const applicantData = findPropById("applicant_data", metadata["credit_props"]);
  await handleApplicantData(applicantData, issuanceInfo.id);
}

async function handleEventData(eventDataJson: any, issuanceInfoId: string, eventIndex: string): Promise<void> {
  const eventData = EventData.create({
    id: `${issuanceInfoId}-${eventIndex}`,
    latitude: findPropById("location", eventDataJson).content.latitude,
    longitude: findPropById("location", eventDataJson).content.longitude,
    amount: findPropById("amount", eventDataJson).content,
    magnitude: findPropById("magnitude", eventDataJson).content,
    registrationDate: findPropById("registration_date", eventDataJson).content,
    issuanceInfoId: issuanceInfoId,
  })
  await eventData.save();
  for (let [i, material] of findPropById("material", eventDataJson).content.entries()) {
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
    const metadataUri = MetadataUri.create({
      id: `${creditCollectionId}-${i}`,
      url: url,
      creditCollectionId: creditCollectionId,
    })
    await metadataUri.save();
  }
}

async function handleMediaFiles(mediaFiles: any, issuanceInfoId: string): Promise<void> {
  for (let [i, mediaFileJson] of mediaFiles.content.entries()) {
    const mediaFile = MediaFile.create({
      id: `${issuanceInfoId}-${i}`,
      url: mediaFileJson.url,
      name: mediaFileJson.name,
      issuanceInfoId: issuanceInfoId,
    })
    await mediaFile.save();
  }
}

async function handleBinaryFiles(binaryFiles: any, issuanceInfoId: string): Promise<void> {
  for (let [i, binaryFileJson] of binaryFiles.content.entries()) {
    const binaryFile = BinaryFile.create({
      id: `${issuanceInfoId}-${i}`,
      url: binaryFileJson.url,
      name: binaryFileJson.name,
      issuanceInfoId: issuanceInfoId,
    })
    await binaryFile.save();
  }
}

async function handleApplicantData(applicantDataJson: any, issuanceInfoId: string): Promise<void> {
  const applicantData = ApplicantData.create({
    id: `${issuanceInfoId}`,
    name: applicantDataJson.content.name,
    description: applicantDataJson.content.description,
    issuanceInfoId: issuanceInfoId,
  })
  await applicantData.save();
  await handleWebReferences(applicantDataJson.content["web_refs"], applicantData.id);
}

async function handleWebReferences(webReferences: any, applicantId: string): Promise<void> {
  for (let [i, url] of webReferences.entries()) {
    const webReference = WebReference.create({
      id: `${applicantId}-${i}`,
      url: url,
      applicantDataId: applicantId,
    })
    await webReference.save();
  }
}