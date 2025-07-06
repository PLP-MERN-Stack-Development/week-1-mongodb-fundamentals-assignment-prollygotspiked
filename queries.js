// queries.js
const { MongoClient } = require('mongodb');

const uri = 'mongodb://localhost:27017'; // Change if using Atlas

const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const books = db.collection(collectionName);

    console.log('Running Queries...\n');

    // 1. Find all books in a specific genre (e.g., "Fiction")
    const fictionBooks = await books.find({ genre: 'Fiction' }).toArray();
    console.log('Books in Fiction genre:');
    fictionBooks.forEach(book => console.log(`- ${book.title}`));

    // 2. Find books published after a certain year (e.g., 1950)
    const recentBooks = await books.find({ published_year: { $gt: 1950 } }).toArray();
    console.log('\nBooks published after 1950:');
    recentBooks.forEach(book => console.log(`- ${book.title} (${book.published_year})`));

    // 3. Find books by a specific author (e.g., "George Orwell")
    const orwellBooks = await books.find({ author: 'George Orwell' }).toArray();
    console.log('\nBooks by George Orwell:');
    orwellBooks.forEach(book => console.log(`- ${book.title}`));

    // 4. Update the price of a specific book (e.g., "1984")
    const updateResult = await books.updateOne(
      { title: '1984' },
      { $set: { price: 13.99 } }
    );
    console.log(`\nUpdated price of "1984": ${updateResult.modifiedCount} document(s) modified`);

    // 5. Delete a book by its title (e.g., "Moby Dick")
    const deleteResult = await books.deleteOne({ title: 'Moby Dick' });
    console.log(`\nDeleted "Moby Dick": ${deleteResult.deletedCount} document(s) deleted`);

    // 6. Find books both in stock and published after 2010
    const inStockRecent = await books.find({
      in_stock: true,
      published_year: { $gt: 2010 }
    }).project({ title: 1, author: 1, price: 1, _id: 0 }).toArray();
    console.log('\nBooks in stock and published after 2010 (projected fields):');
    console.table(inStockRecent);

    // 7. Sort books by price ascending
    const sortedAsc = await books.find({})
      .sort({ price: 1 })
      .project({ title: 1, price: 1, _id: 0 })
      .toArray();
    console.log('\nBooks sorted by price (ascending):');
    console.table(sortedAsc);

    // 8. Sort books by price descending
    const sortedDesc = await books.find({})
      .sort({ price: -1 })
      .project({ title: 1, price: 1, _id: 0 })
      .toArray();
    console.log('\nBooks sorted by price (descending):');
    console.table(sortedDesc);

    // 9. Pagination: 5 books per page, page 1 (skip 0)
    const page1 = await books.find({})
      .skip(0)
      .limit(5)
      .project({ title: 1, author:1, price:1, _id: 0 })
      .toArray();
    console.log('\nPage 1 (5 books):');
    console.table(page1);

    // Pagination: page 2 (skip 5)
    const page2 = await books.find({})
      .skip(5)
      .limit(5)
      .project({ title: 1, author:1, price:1, _id: 0 })
      .toArray();
    console.log('\nPage 2 (5 books):');
    console.table(page2);

    // 10. Aggregation pipeline: average price by genre
    const avgPriceByGenre = await books.aggregate([
      {
        $group: {
          _id: "$genre",
          averagePrice: { $avg: "$price" }
        }
      }
    ]).toArray();
    console.log('\nAverage price by genre:');
    console.table(avgPriceByGenre);

    // 11. Aggregation pipeline: author with the most books
    const topAuthor = await books.aggregate([
      {
        $group: {
          _id: "$author",
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log('\nAuthor with the most books:');
    console.table(topAuthor);

    // 12. Group books by publication decade and count
    const booksByDecade = await books.aggregate([
      {
        $group: {
          _id: {
            decade: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] }
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { "_id.decade": 1 } }
    ]).toArray();
    console.log('\nBooks grouped by publication decade:');
    console.table(booksByDecade);

    // 13. Create index on title field
    const titleIndexResult = await books.createIndex({ title: 1 });
    console.log(`\nCreated index on title: ${titleIndexResult}`);

    // 14. Create compound index on author and published_year
    const compoundIndexResult = await books.createIndex({ author: 1, published_year: 1 });
    console.log(`Created compound index on author and published_year: ${compoundIndexResult}`);

    // 15. Use explain() to show index usage on a title search
    const explainTitle = await books.find({ title: '1984' }).explain('executionStats');
    console.log('\nExplain for title search:');
    console.log(JSON.stringify(explainTitle.executionStats.executionStages, null, 2));

    // 16. Use explain() to show index usage on author and published_year query
    const explainCompound = await books.find({ author: 'George Orwell', published_year: { $gt: 1900 } })
      .explain('executionStats');
    console.log('\nExplain for author and published_year query:');
    console.log(JSON.stringify(explainCompound.executionStats.executionStages, null, 2));

  } catch (error) {
    console.error('Error running queries:', error);
  } finally {
    await client.close();
    console.log('\nDisconnected from MongoDB');
  }
}

runQueries();
