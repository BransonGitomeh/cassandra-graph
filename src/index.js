import express from 'express';
import graphqlHTTP from 'express-graphql';
import {
  GraphQLSchema,
  GraphQLObjectType,
  GraphQLString,
  GraphQLList,
}
from 'graphql';
import datatase from './database';

import {
  ifError,
}
from 'assert';
import {
  map,
}
from 'async';
import {
  database as dbSettings,
}
from './settings';

const host = new GraphQLObjectType({
  name: 'host',
  fields: {
    name: {
      type: GraphQLString,
    },
  },
});

const collumn = new GraphQLObjectType({
  name: 'column',
  fields: {
    name: {
      type: GraphQLString,
    },
    kind: {
      type: GraphQLString,
    },
    position: {
      type: GraphQLString,
    },
    type: {
      type: GraphQLString,
    },
    clustering_order: {
      type: GraphQLString,
    },
  },
});

const table = new GraphQLObjectType({
  name: 'table',
  fields: {
    name: {
      type: GraphQLString,
    },
    comment: {
      type: GraphQLString,
    },
    compaction: {
      type: GraphQLString,
    },
    compression: {
      type: GraphQLString,
    },
    caching: {
      type: GraphQLString,
    },
    columns: {
      type: new GraphQLList(collumn),
    },
  },
});

const keyspace = new GraphQLObjectType({
  name: 'keyspace',
  fields: {
    name: {
      type: GraphQLString,
    },
    tables: {
      type: new GraphQLList(table),
    },
  },
});


const schema = new GraphQLSchema({
  query: new GraphQLObjectType({
    name: 'RootQueryType',
    fields: {
      core: {
        type: new GraphQLObjectType({
          name: 'core',
          fields: {
            hosts: {
              type: new GraphQLList(host),
            },
            keyspaces: {
              type: new GraphQLList(keyspace),
            },
          },
        }),
        resolve(parentValue, args, {
          db,
        }) {
          const data = {
            hosts: [],
            keyspaces: [],
          };
          return new Promise((resolve, reject) => {
            map(Object.keys(db.metadata.keyspaces), (keyspaceKey, next) => {
              const keyspace = {};
              keyspace.name = keyspaceKey;

                // execute cql to get tables
              const tablesQuery = `SELECT * FROM system_schema.tables WHERE keyspace_name = '${keyspaceKey}';`;
              db.execute(tablesQuery, (err, results) => {
                ifError(err);
                keyspace.tables = [];

                map(results.rows, (row, nextTable) => {
                  const tableDetails = {
                    name: row.table_name,
                  };

                  Object.assign(tableDetails, row);


                  const collumnsQuery = `SELECT * FROM system_schema.columns WHERE keyspace_name = '${keyspaceKey}' AND table_name = '${row.table_name}';`;
                  db.execute(collumnsQuery, (resultsTableErr, resultsTable) => {
                    ifError(resultsTableErr);

                    tableDetails.columns = [];
                    resultsTable.rows.map((row) => {
                      console.log(row);
                      tableDetails.columns.push({
                        name: row.column_name,
                        type: row.type,
                        kind: row.kind,
                        clustering_order: row.clustering_order,
                        position: row.position,
                      });
                    });


                    keyspace.tables.push(tableDetails);
                    nextTable();
                  });
                }, (err) => {
                  data.keyspaces.push(keyspace);
                  next();
                });
              });
            },
              err => resolve(data));
          });
        },
      },
    },
  }),
});

const app = express();

datatase.connect(dbSettings).then((db) => {
  app.use('/', graphqlHTTP({
    schema,
    graphiql: true,
    context: {
      db,
    },
  }));
});

app.listen(4000);
