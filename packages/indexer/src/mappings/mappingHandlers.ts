import { ExecuteEvent, Message, Transaction, Block, CreditCollection, EventData, MaterialData, MetadataUri, MediaFile, BinaryFile, ApplicantData, WebReference, CreditData, CreateListingWasmEvent, MarketplaceListing, BuyCreditsWasmEvent, UpdateListingWasmEvent, CancelListingWasmEvent } from "../types";
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

export async function handleCreateListing(event: CosmosEvent): Promise<void> {

  const listingOwner = fetchPropertyFromEvent(event, "listing_owner");
  const denom = fetchPropertyFromEvent(event, "denom");
  const numberOfCredits = BigInt(fetchPropertyFromEvent(event, "number_of_credits"));
  const pricePerCreditAmount = BigInt(fetchPropertyFromEvent(event, "price_per_credit_amount"));
  const pricePerCreditDenom = fetchPropertyFromEvent(event, "price_per_credit_denom");

  const createListingWasmEvent = CreateListingWasmEvent.create({
    id: `${event.tx.hash}-${event.msg.idx}-${event.idx}`,
    listingOwner: listingOwner,
    denom: denom,
    numberOfCredits: numberOfCredits,
    pricePerCreditAmount: pricePerCreditAmount,
    pricePerCreditDenom: pricePerCreditDenom,
  });
  await createListingWasmEvent.save();
  const marketplaceListing = MarketplaceListing.create({
    id: `${listingOwner}-${denom}`,
    owner: listingOwner,
    denom: denom,
    amount: numberOfCredits,
    pricePerCreditAmount: pricePerCreditAmount,
    pricePerCreditDenom: pricePerCreditDenom,
  });
  await marketplaceListing.save();
}

export async function handleUpdateListing(event: CosmosEvent): Promise<void> {

  const listingOwner = fetchPropertyFromEvent(event, "listing_owner");
  const denom = fetchPropertyFromEvent(event, "denom");
  const numberOfCredits = BigInt(fetchPropertyFromEvent(event, "number_of_credits"));
  const pricePerCreditAmount = BigInt(fetchPropertyFromEvent(event, "price_per_credit_amount"));
  const pricePerCreditDenom = fetchPropertyFromEvent(event, "price_per_credit_denom");

  const updateListingWasmEvent = UpdateListingWasmEvent.create({
    id: `${event.tx.hash}-${event.msg.idx}-${event.idx}`,
    listingOwner: listingOwner,
    denom: denom,
    numberOfCredits: numberOfCredits,
    pricePerCreditAmount: pricePerCreditAmount,
    pricePerCreditDenom: pricePerCreditDenom,
  });
  await updateListingWasmEvent.save();

  const marketplaceListing = await MarketplaceListing.get(`${listingOwner}-${denom}`);
  marketplaceListing.amount = numberOfCredits;
  marketplaceListing.pricePerCreditAmount = pricePerCreditAmount;
  marketplaceListing.pricePerCreditDenom = pricePerCreditDenom;
  await marketplaceListing.save();
}

export async function handleCancelListing(event: CosmosEvent): Promise<void> {

  const listingOwner = fetchPropertyFromEvent(event, "listing_owner");
  const denom = fetchPropertyFromEvent(event, "denom");

  const cancelListingWasmEvent = CancelListingWasmEvent.create({
    id: `${event.tx.hash}-${event.msg.idx}-${event.idx}`,
    listingOwner: listingOwner,
    denom: denom,
  });
  await cancelListingWasmEvent.save();

  await MarketplaceListing.remove(`${listingOwner}-${denom}`);
}

export async function handleBuyCredits(event: CosmosEvent): Promise<void> {

  const listingOwner = fetchPropertyFromEvent(event, "listing_owner");
  const denom = fetchPropertyFromEvent(event, "denom");
  const buyer = fetchPropertyFromEvent(event, "buyer");
  const numberOfCreditsBought = BigInt(fetchPropertyFromEvent(event, "number_of_credits_bought"));
  const totalPriceAmount = BigInt(fetchPropertyFromEvent(event, "total_price_amount"));
  const totalPriceDenom = fetchPropertyFromEvent(event, "total_price_denom");

  const buyCreditsWasmEvent = BuyCreditsWasmEvent.create({
    id: `${event.tx.hash}-${event.msg.idx}-${event.idx}`,
    listingOwner: listingOwner,
    denom: denom,
    buyer: buyer,
    numberOfCreditsBought: numberOfCreditsBought,
    totalPriceAmount: totalPriceAmount,
    totalPriceDenom: totalPriceDenom,
  });
  await buyCreditsWasmEvent.save();

  const marketplaceListing = await MarketplaceListing.get(`${buyer}-${denom}`);
  marketplaceListing.amount = marketplaceListing.amount - numberOfCreditsBought;
  if (marketplaceListing.amount === BigInt(0)) {
    await MarketplaceListing.remove(`${buyer}-${denom}`);
  } else {
    await marketplaceListing.save();
  }
}

export async function handleWasmEvents(event: CosmosEvent): Promise<void> {
  const action = fetchPropertyFromEvent(event, "action");
  switch (action) {
    case "create_listing":
      await handleCreateListing(event);
      break;
    case "update_listing":
      await handleUpdateListing(event);
      break;
    case "cancel_listing":
      await handleCancelListing(event);
      break;
    case "buy_credits":
      await handleBuyCredits(event);
      break;
    default:
      break;
  }
}

export async function handleIssueCredits(event: CosmosEvent): Promise<void> {
  let denom = fetchPropertyFromEvent(event, "denom");
  let amount = fetchPropertyFromEvent(event, "amount");
  let projectId = fetchPropertyFromEvent(event, "project_id");
  let creditTypeAbbreviation = fetchPropertyFromEvent(event, "credit_type_abbreviation");

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
    await handleCreditData(metadata, creditCollection.id, i.toString());
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
  const reqUri = "http://51.159.197.8:8080/ipfs/" + url.substring(7);
  const res = await fetch(reqUri);
  return res.json();
}

async function handleCreditData(metadata: any, creditCollectionId: string, creditDataIndex: string): Promise<void> {
  const creditData = CreditData.create({
    id: `${creditCollectionId}-${creditDataIndex}`,
    issuanceDate: findPropById("issuance_date", metadata["credit_props"])?.content,
    creditType: findPropById("credit_type", metadata["credit_props"])?.content,
    // For now, we take only amount from first event
    amount: findPropById("amount", findPropById("credit_events_data", metadata["credit_props"])?.content[0].content)?.content,
    aggregationLatitude: findPropById("aggregation_location", metadata["credit_props"])?.content.latitude || "",
    aggregationLongitude: findPropById("aggregation_location", metadata["credit_props"])?.content.longitude || "",
    creditCollectionId: creditCollectionId,
  })
  await creditData.save();
  const eventData = findPropById("credit_events_data", metadata["credit_props"]);
  for (let [i, event] of eventData.content.entries()) {
    await handleEventData(event.content, creditData.id, i.toString());
  }
  const mediaFiles = findPropById("credit_media", metadata["credit_props"]);
  await handleMediaFiles(mediaFiles, creditData.id);
  const binaryFiles = findPropById("credit_files", metadata["credit_props"]);
  await handleBinaryFiles(binaryFiles, creditData.id);
  const applicantData = findPropById("applicant_data", metadata["credit_props"]);
  await handleApplicantData(applicantData, creditData.id);
}

async function handleEventData(eventDataJson: any, creditDataId: string, eventIndex: string): Promise<void> {
  const eventData = EventData.create({
    id: `${creditDataId}-${eventIndex}`,
    latitude: findPropById("location", eventDataJson)?.content.latitude,
    longitude: findPropById("location", eventDataJson)?.content.longitude,
    amount: findPropById("amount", eventDataJson)?.content,
    magnitude: findPropById("magnitude", eventDataJson)?.content,
    registrationDate: findPropById("registration_date", eventDataJson)?.content,
    creditDataId: creditDataId,
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

async function handleMediaFiles(mediaFiles: any, creditDataId: string): Promise<void> {
  for (let [i, mediaFileJson] of mediaFiles.content.entries()) {
    const mediaFile = MediaFile.create({
      id: `${creditDataId}-${i}`,
      url: mediaFileJson.url,
      name: mediaFileJson.name,
      creditDataId: creditDataId,
    })
    await mediaFile.save();
  }
}

async function handleBinaryFiles(binaryFiles: any, creditDataId: string): Promise<void> {
  for (let [i, binaryFileJson] of binaryFiles.content.entries()) {
    const binaryFile = BinaryFile.create({
      id: `${creditDataId}-${i}`,
      url: binaryFileJson.url,
      name: binaryFileJson.name,
      creditDataId: creditDataId,
    })
    await binaryFile.save();
  }
}

async function handleApplicantData(applicantDataJson: any, creditDataId: string): Promise<void> {
  const applicantData = ApplicantData.create({
    id: `${creditDataId}`,
    name: applicantDataJson.content.name,
    description: applicantDataJson.content.description,
    creditDataId: creditDataId,
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