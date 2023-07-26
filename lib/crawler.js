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
    this.browser = null;
  }

  // Method that handles the request to a URL and returns all the links on the page that start with the original URL
  async handleRequest(url) {

    try {
      if (!this.browser) {
        chromium.setGraphicsMode = false;
        this.browser = await puppeteer.launch({
          executablePath: await chromium.executablePath(),
          headless: chromium.headless,
          ignoreHTTPSErrors: true,
          defaultViewport: chromium.defaultViewport,
          args: [...chromium.args, "--hide-scrollbars", "--disable-web-security"],
        });
      }
      
      const page = await this.browser.newPage();
      await page.goto(url, { waitUntil: 'networkidle0' });
  
      const links = await page.$$eval('a', (anchors, url) => anchors
        .map(anchor => anchor.href)
        .filter(href => href && href.indexOf('#') === -1)
        .map(href => new URL(href, document.location.href).href)
        .filter(href => href.startsWith(url))
      , url);

      await page.close();
  
      return links;
    } catch (error) {
      console.error(`Error while handling request to ${url}:`, error);
      return [];
    } 
  }

  // Method that handles the crawling of a page. It scrapes the page for content, splits the content into chunks, transforms the chunks into embeddings, and stores the embeddings in Pinecone Vector DB
  async handlePageCrawl(url, index, embedder) {

    try {
      if (!this.browser) {
        chromium.setGraphicsMode = false;
        this.browser = await puppeteer.launch({
          executablePath: await chromium.executablePath(),
          headless: chromium.headless,
          ignoreHTTPSErrors: true,
          defaultViewport: chromium.defaultViewport,
          args: [...chromium.args, "--hide-scrollbars", "--disable-web-security"],
        });
      }
  
      const page = await this.browser.newPage();
      await page.goto(url, { waitUntil: 'networkidle0' });

      const title = await page.title();
      const text = await page.evaluate(() => document.body.textContent);

      const pageData = {
        url,
        text,
        title,
      };

      await page.close();
  
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
  
        // Upsert each vector chunk to pinecone
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

          console.log('Crawling URL for links: ', url);

          const newLinks = await this.handleRequest(url);
          console.log('New Links found: ', newLinks.length);

          //add urls to queue, double checking that we dont add it twice
          newLinks.forEach(link => {
            if (!queue.has(link)) {
              queue.add(link);
            }
          });

          //add to visited urls so we dont scrape the same url twice
          this.visitedUrls.add(url);

          counter++;
          if (counter === 10) {
            // Save every 10 URLs to the database
            const result = await namespaces.updateOne(
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

      // Save remaining URLs to the database, and set success to true
      await namespaces.updateOne(
        { teamId: new ObjectId(this.teamId), namespace: namespace },
        { 
          $addToSet: { originalUrls: { $each: Array.from(originalUrls) } },
          $set: { 
            queue: Array.from(queue),
            visitedUrls: Array.from(this.visitedUrls),
            success: true,
          } 
        },
        { upsert: true }
      );      
  
      console.log('Done crawling: ', `Added ${Array.from(queue).length} URLs to the queue...`);
    
      if (this.browser !== null) {
        console.log('Closing browser...');
        try {
          await this.browser.close();
          console.log('Browser closed');
        } catch (error) {
          console.error('Error closing browser:', error);
        }
      }

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

    console.log('Started page content crawl...');

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

          console.log('Finished crawling: ', url);

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

    if (this.browser !== null) {
      console.log('Closing browser...');
      try {
        await this.browser.close();
        console.log('Browser closed');
      } catch (error) {
        console.error('Error closing browser:', error);
      }
    }

    return true;
  }
  
}

export { Crawler };