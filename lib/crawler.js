import htmlToMd from 'html-to-md';
import { ObjectId } from 'mongodb';
import { Document } from "langchain/document";
import { RecursiveCharacterTextSplitter } from "langchain/text_splitter";
import { v4 as uuid } from 'uuid';
import puppeteer from "puppeteer-core";
import chromium from "@sparticuz/chromium"

// Function to truncate a string to a specified number of bytes
const truncateStringByBytes = (str, bytes) => {
  const enc = new TextEncoder();
  return new TextDecoder("utf-8").decode(enc.encode(str).slice(0, bytes));
};

// Function to slice an array into chunks of a specified maximum size in bytes
const sliceIntoChunks = (arr, maxSizeInBytes) => {
  let chunks = [];
  let currentChunk = [];
  let currentChunkSize = 0;

  for (let item of arr) {
    let itemSize = Buffer.byteLength(JSON.stringify(item), 'utf8'); // Get size of item in bytes

    if (currentChunkSize + itemSize > maxSizeInBytes) {
      // If adding the item would exceed the max size, start a new chunk
      chunks.push(currentChunk);
      currentChunk = [];
      currentChunkSize = 0;
    }

    // Add the item to the current chunk
    currentChunk.push(item);
    currentChunkSize += itemSize;
  }

  // Add the last chunk if it's not empty
  if (currentChunk.length > 0) {
    chunks.push(currentChunk);
  }

  return chunks;
};


// Crawler class that handles the scraping and processing of web content
class Crawler {
  constructor(urls, limit = 1000, textLengthMinimum = 200, namespace, db, teamId) {
    this.pages = [];
    this.urls = urls;
    this.limit = limit;
    this.textLengthMinimum = textLengthMinimum;
    this.count = 0;
    this.visitedUrls = new Set([]);
    this.namespace = namespace;
    this.db = db;
    this.teamId = teamId;
  }

  // Method that handles the request to a URL and returns all the links on the page that start with the original URL
  async handleRequest(url) {
    let browser = null;
    try {
      browser = await puppeteer.launch({
        executablePath: await chromium.executablePath(),
        headless: chromium.headless,
        ignoreHTTPSErrors: true,
        defaultViewport: chromium.defaultViewport,
        args: [...chromium.args, "--hide-scrollbars", "--disable-web-security"],
      });
  
      const page = await browser.newPage();
      await page.goto(url, { waitUntil: 'networkidle0' });
  
      const links = await page.$$eval('a', (anchors, url) => anchors
        .map(anchor => anchor.href)
        .filter(href => href && href.indexOf('#') === -1)
        .map(href => new URL(href, document.location.href).href)
        .filter(href => href.startsWith(url))
      , url);

      console.log('Found links: ',links)
  
      return links;
    } catch (error) {
      console.error(`Error while handling request to ${url}:`, error);
      return [];
    } finally {
      if (browser !== null) {
        await browser.close();
      }
    }
  }

  // Method that handles the crawling of a page. It scrapes the page for content, splits the content into chunks, transforms the chunks into embeddings, and stores the embeddings in Pinecone Vector DB
  async handlePageCrawl(url, index, embedder) {
    let browser = null;
    try {
      browser = await puppeteer.launch({
        executablePath: await chromium.executablePath(),
        headless: chromium.headless,
        ignoreHTTPSErrors: true,
        defaultViewport: chromium.defaultViewport,
        args: [...chromium.args, "--hide-scrollbars", "--disable-web-security"],
      });
  

      const page = await browser.newPage();
      await page.goto(url, { waitUntil: 'networkidle0' });

      const title = await page.title();
      const text = await page.evaluate(() => document.body.textContent);

      const pageData = {
        url,
        text,
        title,
      };
  
      if (text.length > this.textLengthMinimum) {
        const splitter = new RecursiveCharacterTextSplitter({
          chunkSize: 300,
          chunkOverlap: 20,
        });
  
        const docs = await splitter.splitDocuments([
          new Document({ pageContent: pageData.text, metadata: { url: pageData.url, text: truncateStringByBytes(pageData.text, 36000) } }),
        ]);
  
        const vectors = await Promise.all(docs.map(async doc => {
          const embedding = await embedder.embedQuery(doc.pageContent)
          return {
            id: uuid(),
            values: embedding,
            metadata: {
              chunk: doc.pageContent,
              text: doc.metadata.text,
              url: doc.metadata.url,
            }
          };
        }));
  
        // Split vectors into chunks of no more than 1.9MB
        const chunks = sliceIntoChunks(vectors, 1.9 * 1024 * 1024); 
  
        // Upsert each chunk
        await Promise.all(chunks.map(async chunk => {
          await index.upsert({
            upsertRequest: {
              vectors: chunk,
              namespace: this.namespace
            }
          });
        }));
      }
  
      return;
    } catch (error) {
      console.error(`Error while handling request to ${url}:`, error);
      return;
    } finally {
      if (browser !== null) {
        await browser.close();
      }
    }
  }
  
  // Method that handles the crawling of URLs. It adds all the URLs that have been crawled to a MongoDB collection
  async crawlForUrls() {

    const namespace = this.namespace; 
    const originalUrls = new Set(this.urls);
    const queue = new Set(this.urls);
    let counter = 0;
  
    try {
  
      const namespaces = this.db.collection('namespaces');
  
      for (let url of queue) {
        if (!this.visitedUrls.has(url)) {
          const newLinks = await this.handleRequest(url);
          //console.log('crawling', `Adding ${newLinks.length} new links to the queue`);
          newLinks.forEach(link => {
            if (!queue.has(link)) {
              queue.add(link);
            }
          });
          this.visitedUrls.add(url);

          counter++;
          if (counter === 20) {
            // Save every 20 URLs to the database
            await namespaces.updateOne(
              { teamId: new ObjectId(this.teamId), namespace: namespace },
              { 
                $addToSet: { originalUrls: { $each: Array.from(originalUrls) } },
                $set: { 
                  queue: Array.from(queue),
                  visitedUrls: Array.from(this.visitedUrls)
                } 
              },
              { upsert: true }
            );
            counter = 0;
          }
        }
      }

      await namespaces.updateOne(
        { teamId: new ObjectId(this.teamId), namespace: namespace },
        { 
          $set: { 
            success: true,
          } 
        },
        { upsert: true }
      );
  
      console.log('Done crawling: ', `Added ${Array.from(queue).length} URLs to the queue...`);
    
      return;
    } catch (error) {
      console.error(`Error while crawling urls:`, error);
      throw error;
    }
    
  }

  // Method that starts the crawling of pages. It loads all the URLs that have been crawled into memory and starts crawling the pages
  startPageCrawl = async (index, embedder) => {

    // Load all the URLs that have been crawled for this team into memory.
    const namespaces = this.db.collection('namespaces');
    const namespaceData = await namespaces.findOne({ namespace: this.namespace });
    const crawledUrls = new Set(namespaceData?.crawledUrls || []);
    const queue = namespaceData?.queue || [];

    for (let i = 0; i < queue.length && this.count < this.limit; i++) {

      const url = queue[i];
      
      if (url && namespaceData && !crawledUrls.has(url)) {
        try {
          // Add the URL to the team's crawled URLs.
          await this.handlePageCrawl(url, index, embedder);
          
          // Add the URL to the visitedUrls and crawledUrls sets AFTER crawling
          crawledUrls.add(url);  // Update the in-memory set.
          this.count += 1;

          // Update the team's crawled URLs in the database.
          await namespaces.updateOne(
            { _id: namespaceData._id },
            { 
              $addToSet: { crawledUrls: url },
              $unset: {error: ""},
              $set: {finishedUpsert: false}
            },
            { upsert: true }
          );

          //Add a delay between requests
          await new Promise(resolve => setTimeout(resolve, 500));

        } catch (error) {
          console.error(`Error while crawling ${url}:`, error);
          throw error;
        }
      }
    }

    // Update the team's crawled URLs in the database.
    await namespaces.updateOne(
      { _id: namespaceData._id },
      { 
        $set: {finishedUpsert: true}
      },
      { upsert: true }
    );

    return true;
  }
  
}

export { Crawler };