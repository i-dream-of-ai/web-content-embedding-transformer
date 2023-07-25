import { PineconeClient } from "@pinecone-database/pinecone";
import { OpenAIEmbeddings } from "langchain/embeddings/openai";
import { Crawler } from './lib/crawler.js';
import { connectToDatabase } from './lib/db.js';
import { ObjectId } from 'mongodb';

const retry = async (fn, retries = 1) => {
  try {
    return await fn();
  } catch (error) {
    if (retries > 0) {
      return await retry(fn, retries - 1);
    } else {
      throw error;
    }
  }
};

async function saveErrorToDB(teamId, namespace, error){
  const { db } = await connectToDatabase();
  const namespaces = db.collection('namespaces');
    await namespaces.updateOne(
      { teamId: new ObjectId(teamId), namespace: namespace },
      { 
        $set: { 
          error: error
        } 
      },
      { upsert: true }
    );
}

export async function handler(event, context) {
  
  const { limit, namespace, teamId, urls } = event;
  const crawlLimit = parseInt(limit);

  try {

    if(!namespace){
        await saveErrorToDB(teamId, namespace, { 
          message: "You must include a namespace string." 
        })
        console.error("No namespace found in body");
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "You must include a namespace string." })
        };
    }

    if (Array.isArray(namespace)) {
      console.error("The namespace was an array. It needs to be a single string: crawl.ts");
      await saveErrorToDB(teamId, namespace, { 
        message: "The namespace was an array. It needs to be a single string." 
      })
      return {
          statusCode: 500,
          body: JSON.stringify({ message: "The namespace was an array. It needs to be a single string." })
      };
    }
  
    if(!teamId){
      console.error("No teamId found in body.");
      await saveErrorToDB(teamId, namespace, { 
        message: "You must include a teamId." 
      })
      return {
          statusCode: 500,
          body: JSON.stringify({ message: 'You must include a teamId.' })
      };
    }
  
    if (Array.isArray(teamId)) {
      console.error("The teamId was an array. It needs to be a single string.");
      await saveErrorToDB(teamId, namespace, { 
        message: "The teamId was an array. It needs to be a single string." 
      });
      return {
          statusCode: 500,
          body: JSON.stringify({ message: "The teamId was an array. It needs to be a single string." })
      };
    }
  
    const { db } = await connectToDatabase();
    const namespaces = db.collection('namespaces');

    if(urls && urls.length){
      console.log('urls',urls)
      try {
        const crawler = new Crawler(urls, undefined, 200, namespace, db, teamId)
        await crawler.crawlForUrls();
      } catch (e) {
        await saveErrorToDB(teamId, namespace, { 
          message: "Error when crawling for URLs.",
          error: e
        });
        return {
          statusCode: 500,
          body: JSON.stringify({ message: JSON.stringify(e) })
        };
      }
      
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Crawl Complete. Saved urls to database.' })
      };
  
    } else {

      if (!process.env.PINECONE_INDEX_NAME) {
        return {
          statusCode: 500,
          body: JSON.stringify({ message: "PINECONE_INDEX_NAME not set" })
        };
      }
        
      try {

        const pinecone = new PineconeClient();
        await pinecone.init({
          environment: process.env.PINECONE_ENVIRONMENT,
          apiKey: process.env.PINECONE_API_KEY,
        });
        console.log("init pinecone");

        const embedder = new OpenAIEmbeddings({
          modelName: "text-embedding-ada-002"
        });

        const index = pinecone && pinecone.Index(process.env.PINECONE_INDEX_NAME);

        const crawler = new Crawler([], crawlLimit, 200, namespace, db, teamId);

        await crawler.startPageCrawl(index, embedder);

      } catch (e) {
        console.error(e);
        await namespaces.updateOne(
            { 
              teamId: new ObjectId(teamId), 
              namespace: namespace 
            },
            { 
              $set: {
                error: { 
                  message: "An error occured when when crawling for content. Please email support support@idreamofai.com",
                  error: e
                },
                finishedUpsert: true
              }
            },
            { upsert: true }
          );
        return {
          statusCode: 500,
          body: JSON.stringify({ message: JSON.stringify(e) })
        };
      }

      await namespaces.updateOne(
        { teamId: new ObjectId(teamId), namespace: namespace },
        { 
          $set: { 
            success: true,
            message: 'Upload Complete. Saved documents to vectore database.',
            finishedUpsert: true
          }
        },
        { upsert: true }
      );
  
      return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Upload Complete. Saved documents to vector database.' })
      };
    }
    
  } catch (error) { 
    console.error(error);
    const { db } = await connectToDatabase();
    const namespaces = db.collection('namespaces');
    await namespaces.updateOne(
      { namespace: namespace },
      { 
        $set: { 
          finishedUpsert: true,
          error: { 
            message: "Error in Lambda function WebCrawl. Please email support at support@idreamofai.com.",
            error: error
          }
        } 
      },
      { upsert: true }
    );
    return {
      statusCode: 500,
      body: JSON.stringify(error)
    };
  }
};