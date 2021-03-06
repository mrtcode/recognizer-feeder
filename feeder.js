/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2017 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const mysql2 = require('mysql2');
const mysql2Promise = require('mysql2/promise');
const sqlite = require('sqlite');
const through2 = require('through2');
const request = require('request');
const config = require('./config');

let indexed = 0;
let indexedTotal = 0;
let failedShards = 0;
let done = false;

let shardRows;
let shardRowNr = 0;
let activeShards = [];

async function getShardDate(db, shardID) {
	let row = await db.get('SELECT shardDate FROM shards WHERE shardID = ?', [shardID]);
	if (!row) return new Date(0).toISOString();
	return row.shardDate;
}

async function setShardDate(db, shardID, shardDate) {
	await db.run('INSERT OR REPLACE INTO shards (shardID, shardDate) VALUES (?,?)',
		[shardID, shardDate]);
}

function index(batch, callback) {
	if (!batch.length) return callback();
	request({
		url: config.indexerURL,
		method: 'POST',
		json: batch,
		timeout: 0
	}, function (err, res) {
		if (err) return callback(err);
		callback()
	});
}

function streamShard(connection, shardDateFrom) {
	return new Promise(function (resolve, reject) {
		// Maximum authors string length is 2047 bytes, and when it's 2048 we ignore it
		connection.query("SET SESSION group_concat_max_len = 2048", function (err) {
			if (err) return reject(err);
			
			let shardDate = new Date(0).toISOString(); // 1970-01-01T00:00:00.000Z
			let sql;
			let params;
			
			if (shardDateFrom > shardDate) {
				sql = `
					(SELECT I.itemID, ID_Title.value AS title,
					(
					   SELECT GROUP_CONCAT(CONCAT(creators.firstName, '\\t', creators.lastName)
					                       ORDER BY IC.orderIndex
					                       SEPARATOR '\\n')
					   FROM creators JOIN itemCreators IC USING (creatorID)
					   WHERE IC.itemID=I.itemID
					) AS authors,
					ID_Abstract.value AS abstract,
					ID_Date.value AS date,
					ID_DOI.value AS doi,
					ID_ISBN.value AS isbn,
					ID_Extra.value AS extra,
					IA.storageHash,
					I.serverDateModified AS shardDate1,
					IAI.serverDateModified AS shardDate2
					FROM items I
					JOIN itemData ID_Title ON (ID_Title.itemID = I.itemID AND ID_Title.fieldID IN (110,111,112,113))
					LEFT JOIN itemData ID_Abstract ON (ID_Abstract.itemID = I.itemID AND ID_Abstract.fieldID=90)
					LEFT JOIN itemData ID_Date ON (ID_Date.itemID = I.itemID AND ID_Date.fieldID=14)
					LEFT JOIN itemData ID_DOI ON (ID_DOI.itemID = I.itemID AND ID_DOI.fieldID=26)
					LEFT JOIN itemData ID_ISBN ON (ID_ISBN.itemID = I.itemID AND ID_ISBN.fieldID=11)
					LEFT JOIN itemData ID_Extra ON (ID_Extra.itemID = I.itemID AND ID_Extra.fieldID=22)
					LEFT JOIN itemAttachments IA ON (IA.sourceItemID = I.itemID AND IA.mimeType = 'application/pdf' AND IA.storageHash IS NOT NULL)
					LEFT JOIN items IAI ON (IAI.itemID = IA.itemID)
					WHERE I.serverDateModified >= ?
					AND I.itemTypeID NOT IN (1,14)
					GROUP BY I.itemID)
					
					UNION
					
					(SELECT I.itemID, ID_Title.value AS title,
					(
					   SELECT GROUP_CONCAT(CONCAT(creators.firstName, '\\t', creators.lastName)
					                       ORDER BY IC.orderIndex
					                       SEPARATOR '\\n')
					   FROM creators JOIN itemCreators IC USING (creatorID)
					   WHERE IC.itemID=I.itemID
					) AS authors,
					ID_Abstract.value AS abstract,
					ID_Date.value AS date,
					ID_DOI.value AS doi,
					ID_ISBN.value AS isbn,
					ID_Extra.value AS extra,
					IA.storageHash,
					I.serverDateModified AS shardDate1,
					IAI.serverDateModified AS shardDate2
					FROM items I
					JOIN itemData ID_Title ON (ID_Title.itemID = I.itemID AND ID_Title.fieldID IN (110,111,112,113))
					LEFT JOIN itemData ID_Abstract ON (ID_Abstract.itemID = I.itemID AND ID_Abstract.fieldID=90)
					LEFT JOIN itemData ID_Date ON (ID_Date.itemID = I.itemID AND ID_Date.fieldID=14)
					LEFT JOIN itemData ID_DOI ON (ID_DOI.itemID = I.itemID AND ID_DOI.fieldID=26)
					LEFT JOIN itemData ID_ISBN ON (ID_ISBN.itemID = I.itemID AND ID_ISBN.fieldID=11)
					LEFT JOIN itemData ID_Extra ON (ID_Extra.itemID = I.itemID AND ID_Extra.fieldID=22)
					LEFT JOIN itemAttachments IA ON (IA.sourceItemID = I.itemID AND IA.mimeType = 'application/pdf' AND IA.storageHash IS NOT NULL)
					LEFT JOIN items IAI ON (IAI.itemID = IA.itemID)
					WHERE IAI.serverDateModified >= ?
					AND I.itemTypeID NOT IN (1,14)
					GROUP BY I.itemID);
				`;
				params = [shardDateFrom, shardDateFrom];
			}
			else {
				sql = `
					(SELECT I.itemID, ID_Title.value AS title,
					(
					   SELECT GROUP_CONCAT(CONCAT(creators.firstName, '\\t', creators.lastName)
					                       ORDER BY IC.orderIndex
					                       SEPARATOR '\\n')
					   FROM creators JOIN itemCreators IC USING (creatorID)
					   WHERE IC.itemID=I.itemID
					) AS authors,
					ID_Abstract.value AS abstract,
					ID_Date.value AS date,
					ID_DOI.value AS doi,
					ID_ISBN.value AS isbn,
					ID_Extra.value AS extra,
					IA.storageHash,
					I.serverDateModified AS shardDate1,
					IAI.serverDateModified AS shardDate2
					FROM items I
					JOIN itemData ID_Title ON (ID_Title.itemID = I.itemID AND ID_Title.fieldID IN (110,111,112,113))
					LEFT JOIN itemData ID_Abstract ON (ID_Abstract.itemID = I.itemID AND ID_Abstract.fieldID=90)
					LEFT JOIN itemData ID_Date ON (ID_Date.itemID = I.itemID AND ID_Date.fieldID=14)
					LEFT JOIN itemData ID_DOI ON (ID_DOI.itemID = I.itemID AND ID_DOI.fieldID=26)
					LEFT JOIN itemData ID_ISBN ON (ID_ISBN.itemID = I.itemID AND ID_ISBN.fieldID=11)
					LEFT JOIN itemData ID_Extra ON (ID_Extra.itemID = I.itemID AND ID_Extra.fieldID=22)
					LEFT JOIN itemAttachments IA ON (IA.sourceItemID = I.itemID AND IA.mimeType = 'application/pdf' AND IA.storageHash IS NOT NULL)
					LEFT JOIN items IAI ON (IAI.itemID = IA.itemID)
					AND I.itemTypeID NOT IN (1,14)
					GROUP BY I.itemID)
				`;
				params = [];
			}
			
			let batch = [];
			
			connection.query(sql, params)
				.stream({highWaterMark: 10000})
				.pipe(through2({objectMode: true}, function (row, enc, next) {
					
					if (!row.authors) return next();
					
					if (row.shardDate1 && row.shardDate1.toISOString() > shardDate) {
						shardDate = row.shardDate1.toISOString();
					}
					
					if (row.shardDate2 && row.shardDate2.toISOString() > shardDate) {
						shardDate = row.shardDate2.toISOString();
					}
					
					let res;
					let identifiers = [];
					
					if (row.doi) {
						res = row.doi.match(/(10\.(\d)+\/(\S)+)/);
						if (res) {
							identifiers.push('doi:' + res[1]);
						}
					}
					
					if (row.isbn) {
						res = row.isbn.replace(/[^0-9X ]/g, '').match(/(^| )([0-9X]{10})( |$)|(^| )([0-9]{13})( |$)/);
						if (res) {
							identifiers.push('isbn:' + (res[2] || res[5]));
						}
					}
					
					if (row.extra) {
						res = row.extra.match(/PMID:\s([0-9]{1,9})/);
						if (res) {
							identifiers.push('pmid:' + res[1]);
						}
						
						res = row.extra.match(/PMCID:\s(PMC[0-9]{1,9})/);
						if (res) {
							identifiers.push('pmcid:' + res[1]);
						}
					}
					
					identifiers = identifiers.join('\n');
					
					let year = null;
					
					if (row.date) {
						res = row.date.match(/[0-9]{4}/);
						if (res) {
							year = res[0];
						}
					}
					
					batch.push({
						title: row.title,
						authors: row.authors,
						abstract: row.abstract || undefined,
						year: year || undefined,
						identifiers: identifiers || undefined,
						hash: row.storageHash || undefined
					});
					
					if (batch.length >= 100) {
						index(batch, function (err) {
							connection.close();
							if (err) return reject(err);
							indexed += batch.length;
							batch = [];
							next();
						});
					}
					else {
						next();
					}
					
				}))
				.on('data', function () {
				})
				.on('end', function () {
					index(batch, function (err) {
						connection.close();
						if (err) return reject(err);
						indexed += batch.length;
						resolve(shardDate);
					});
				});
		});
	});
}

async function createReaderConnection(connectionInfo) {
	return new Promise(function (resolve, reject) {
		let connection = mysql2.createConnection(connectionInfo);
		connection.connect(function (err) {
			if (err) return reject(err);
			connection.query("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only'", function (err, result) {
				if (err) return reject(err);
				if (result[0] && result[0].Value === 'ON') return resolve(connection);
				connection.close();
				return resolve();
			});
		});
	});
}

async function getShardReaderConnection(master, shardHostID, shardDb) {
	let [replicas] = await master.execute(
		"SELECT * FROM shardHostReplicas WHERE shardHostID = ? AND state = 'up'",
		[shardHostID]
	);
	
	let i = replicas.length;
	while (--i) {
		let j = Math.floor(Math.random() * (i + 1));
		let tmp = replicas[i];
		replicas[i] = replicas[j];
		replicas[j] = tmp;
	}
	
	for (let i = 0; i < replicas.length; i++) {
		try {
			let replica = replicas[i];
			let connection = await createReaderConnection({
				host: replica.address,
				user: config.masterUser,
				password: config.masterPassword,
				port: replica.port,
				database: shardDb
			});
			if (connection) return connection;
		}
		catch (err) {
			console.log(err);
		}
	}
}

async function worker(master, nr, db) {
	console.log('worker(' + nr + ') started');
	
	while (shardRowNr < shardRows.length) {
		let shardRow = shardRows[shardRowNr++];
		console.log('worker(' + nr + ') taking shard ' + shardRow.shardID);
		
		activeShards.push(shardRow.shardID);
		
		try {
			let connection = await getShardReaderConnection(master, shardRow.shardHostID, shardRow.db);
			if (!connection) throw new Error('failed to get a reader connection for shard ' + shardRow.shardID);
			let shardDate = await getShardDate(db, shardRow.shardID);
			shardDate = await streamShard(connection, shardDate);
			await setShardDate(db, shardRow.shardID, shardDate);
		}
		catch (err) {
			failedShards++;
			console.log(err);
		}
		
		activeShards.splice(activeShards.indexOf(shardRow.shardID), 1);
	}
	
	console.log('worker(' + nr + ') finished');
}

async function main() {
	console.time("total time");
	
	let db = await sqlite.open('./db.sqlite', {Promise});
	await db.run("CREATE TABLE IF NOT EXISTS shards (shardID INTEGER PRIMARY KEY, shardDate TEXT)");
	
	let master = await mysql2Promise.createConnection({
		host: config.masterHost,
		user: config.masterUser,
		password: config.masterPassword,
		database: config.masterDatabase
	});
	
	[shardRows] = await master.execute(
		"SELECT * FROM shards WHERE state = 'up' ORDER BY shardID"
	);
	
	let workers = [];
	for (let i = 1; i <= config.workers; i++) {
		workers.push(worker(master, i, db));
	}
	
	await Promise.all(workers);
	
	master.close();
	
	await db.close();
	console.timeEnd("total time");
	done = true;
}

setInterval(function () {
	indexedTotal += indexed;
	console.log('streaming from shards: [' + activeShards.join(',') + '], failed shards: ' + failedShards + ', indexed total: ' + indexedTotal + ', indexed per second: ' + Math.floor(indexed / 1));
	indexed = 0;
	if (done) {
		process.exit(0);
	}
}, 1000);

main();
