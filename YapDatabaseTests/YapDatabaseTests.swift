//
//  YapDatabaseTests.swift
//  YapDatabaseTests
//
//  Created by Patrick Rogers MC on 2021-01-29.
//  Copyright © 2021 Deusty LLC. All rights reserved.
//

import XCTest

import YapDatabase

class YapDatabaseTests: XCTestCase {
    public static var database: YapDatabase? = nil
    public static var connection: YapDatabaseConnection? = nil

    private static var databaseURL: URL {
        let documentsDirectories = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)
        var databaseURL = documentsDirectories[0]
        databaseURL.appendPathComponent("TestDatabase.sqlite")
        return databaseURL
    }

    override open class func setUp() {
        // Clean up any database from a prior test that was aborted
        try? FileManager.default.removeItem(at: YapDatabaseTests.databaseURL)
        
        self.database = YapDatabase(url: YapDatabaseTests.databaseURL)
        self.connection = database?.newConnection()
        self.database?.register(YapDatabaseRelationship(), withName: "relationships")
    }

    override open class func tearDown() {
        self.database = nil
        self.connection = nil
        try? FileManager.default.removeItem(at: YapDatabaseTests.databaseURL)
    }
    
    override func setUpWithError() throws {
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.removeAllObjectsInAllCollections()
        }
    }
    
    func testSetup() throws {
        XCTAssertNotNil(YapDatabaseTests.database)
        XCTAssertNotNil(YapDatabaseTests.connection)
    }

    // Issue #3824 Triggers a bug where deleting one object causes an edge to fire and
    // delete an unrelated object. There are several versions of this test. The object
    // model for all of them is as follows:
    //
    // unrelated:1
    // owner:1 => property:1
    // owner:4 => property:7
    //
    // In the tests that fail property:1 gets deleted despite the fact that it
    // is not explicitly deleted and owner:1 that is related to it is not deleted.
    //
    // One interesting observation. I put a breakpoint in
    // YapDatabaseTransaction.removeObjectForCollectionKey:withRowid:
    //
    // at line 5400 to see what triggers the deletion. In our application the rowid value that
    // is passed in is not the rowid of that matches the collection key that is passed in. In
    // this test the rowid is the correct value for the collection key, but the row should not be
    // deleted since it isn't the target of a relationship. I'm not sure if this is two similar,
    // but subtly different bugs, or if it is one bug and the rowid discrepency was a red herring.
    func testEdgeBug() throws {
        let ownerCollection = "Owner"
        let propertyCollection = "Property"
        let unrelatedCollection = "Unrelated"

        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)

        // Create an object that is not related to anything
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.setObject("Unrelated 1", forKey: "1", inCollection: unrelatedCollection)
        }

        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: ["1"], inCollection: unrelatedCollection)

        // Create some objects that are related by edges
        YapDatabaseTests.connection?.readWrite { transaction in
            // Remove the unrelated object
            // NOTE Removing unrelated:1 should not affect instances of
            //      ownerCollection or propertyCollection, since they are not
            //      related to unrelatedCollection. But this test also causes
            //      propertyCollection:1 to be deleted. testEdgeBugNoRemoveUnrelated
            //      does not cause this to be deleted, so this is a necessary action
            //      for this bug.
            transaction.removeObject(forKey: "1", inCollection: unrelatedCollection)
        
            transaction.setObject("Owner 1", forKey: "1", inCollection: ownerCollection)
            transaction.setObject("Property 1", forKey: "1", inCollection: propertyCollection)

            transaction.setObject("Owner 4", forKey: "4", inCollection: ownerCollection)
            transaction.setObject("Property 7", forKey: "7", inCollection: propertyCollection)

            // NOTE These edges form relationships between instances of ownerCollection and
            //      propertyCollection and should not affect instances of unrelatedCollection.
            //      But this test also causes property:1 to be deleted despite
            //      the fact that owner:1 has not been deleted. The test testEdgeBugNoEdges
            //      does not have this side effect, so this is a necessary action for this bug.
            guard let relTransaction = transaction.ext("relationships") as? YapDatabaseRelationshipTransaction else {
                print("Error: relationship extension is required")
                return
            }

            let edge1 = YapDatabaseRelationshipEdge(name: "edgeName1",
                                                   sourceKey: "1", collection: ownerCollection,
                                                   destinationKey: "1", collection: propertyCollection,
                                                   nodeDeleteRules: .deleteDestinationIfSourceDeleted)
            relTransaction.add(edge1)

            let edge2 = YapDatabaseRelationshipEdge(name: "edgeName2",
                                                   sourceKey: "4", collection: ownerCollection,
                                                   destinationKey: "7", collection: propertyCollection,
                                                   nodeDeleteRules: .deleteDestinationIfSourceDeleted)
            relTransaction.add(edge2)
        }

        assert(expectedKeys: ["1", "4"], inCollection: ownerCollection)
        assert(expectedKeys: ["1", "7"], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)
    }

    // The only difference between this test and testEdgeBug is that this test
    // doesn't delete unrelated:1. Since unrelated:1 isn't related to
    // anything else, no other objects should be affected by this change, but in
    // the test that fails propetyCollection:1 is deleted as well.
    func testEdgeBugNoRemoveUnrelated() throws {
        let ownerCollection = "Owner"
        let propertyCollection = "Property"
        let unrelatedCollection = "Unrelated"

        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)

        // Create an object that is not related to anything
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.setObject("Unrelated 1", forKey: "1", inCollection: unrelatedCollection)
        }

        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: ["1"], inCollection: unrelatedCollection)

        // Create some objects that are related by edges
        YapDatabaseTests.connection?.readWrite { transaction in
            // NOTE In testEdgeBug, unrelated:1 is removed at this point. The only
            //      difference between the inputs of that test and this one is that it isn't
            //      removed here. But that test deletes property:1

            transaction.setObject("Owner 1", forKey: "1", inCollection: ownerCollection)
            transaction.setObject("Property 1", forKey: "1", inCollection: propertyCollection)

            transaction.setObject("Owner 4", forKey: "4", inCollection: ownerCollection)
            transaction.setObject("Property 7", forKey: "7", inCollection: propertyCollection)

            guard let relTransaction = transaction.ext("relationships") as? YapDatabaseRelationshipTransaction else {
                print("Error: relationship extension is required")
                return
            }

            let edge1 = YapDatabaseRelationshipEdge(name: "edgeName1",
                                                   sourceKey: "1", collection: ownerCollection,
                                                   destinationKey: "1", collection: propertyCollection,
                                                   nodeDeleteRules: .deleteDestinationIfSourceDeleted)
            relTransaction.add(edge1)

            let edge2 = YapDatabaseRelationshipEdge(name: "edgeName2",
                                                   sourceKey: "4", collection: ownerCollection,
                                                   destinationKey: "7", collection: propertyCollection,
                                                   nodeDeleteRules: .deleteDestinationIfSourceDeleted)
            relTransaction.add(edge2)
        }

        assert(expectedKeys: ["1", "4"], inCollection: ownerCollection)
        assert(expectedKeys: ["1", "7"], inCollection: propertyCollection)
        assert(expectedKeys: ["1"], inCollection: unrelatedCollection)
    }

    // The only difference between this test and testEdgeBug is that this test
    // doesn't create any edges. Since the edges don't involve unrelated:1
    // no other objects should be affected by this change, but in the test that fails
    // propetyCollection:1 is deleted as well.
    func testEdgeBugNoEdges() throws {
        let ownerCollection = "Owner"
        let propertyCollection = "Property"
        let unrelatedCollection = "Unrelated"

        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)

        // Create an object that is not related to anything
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.setObject("Unrelated 1", forKey: "1", inCollection: unrelatedCollection)
        }

        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: ["1"], inCollection: unrelatedCollection)

        // Create some objects that are related by edges
        YapDatabaseTests.connection?.readWrite { transaction in
            // Remove the unrelated object
            transaction.removeObject(forKey: "1", inCollection: unrelatedCollection)
        
            transaction.setObject("Owner 1", forKey: "1", inCollection: ownerCollection)
            transaction.setObject("Property 1", forKey: "1", inCollection: propertyCollection)

            transaction.setObject("Owner 4", forKey: "4", inCollection: ownerCollection)
            transaction.setObject("Property 7", forKey: "7", inCollection: propertyCollection)

            // NOTE In testEdgeBug, edges are created between owner and property objects here.
            //      The only difference between the inputs of that test and this one is that these
            //      edges are not created here. But that test deletes property:1 despite
            //      the fact that owner:1 has not been deleted.
        }

        assert(expectedKeys: ["1", "4"], inCollection: ownerCollection)
        assert(expectedKeys: ["1", "7"], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)
    }

    // This test attempts to replicate the full interaction that is exhibiting edge behaviour
    // in my application. While developing this test I discovered a shorter way to trigger the
    // issue that doesn't require resurrecting the object. I'm keeping this test in, because
    // any fix should work in this workflow as well.
    func testEdgeBugLong() throws {
        let rootCollection = "Root"
        let ownerCollection = "Owner"
        let propertyCollection = "Property"
        let unrelatedCollection = "Unrelated"

        // Create some root objects
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.setObject("Root Object 1", forKey: "1", inCollection: rootCollection)
            transaction.setObject("Root Object 2", forKey: "2", inCollection: rootCollection)
        }

        assert(expectedKeys: ["1", "2"], inCollection: rootCollection)
        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)

        // Create an object that is not related to anything
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.setObject("Unrelated 5", forKey: "5", inCollection: unrelatedCollection)
        }

        assert(expectedKeys: ["1", "2"], inCollection: rootCollection)
        assert(expectedKeys: [], inCollection: ownerCollection)
        assert(expectedKeys: [], inCollection: propertyCollection)
        assert(expectedKeys: ["5"], inCollection: unrelatedCollection)

        // Create some objects that are related by edges
        YapDatabaseTests.connection?.readWrite { transaction in
            // Remove the unrelated object
            transaction.removeObject(forKey: "5", inCollection: unrelatedCollection)
        
            transaction.setObject("Owner 2", forKey: "2", inCollection: ownerCollection)
            transaction.setObject("Property 3", forKey: "3", inCollection: propertyCollection)

            transaction.setObject("Owner 4", forKey: "4", inCollection: ownerCollection)
            transaction.setObject("Property 7", forKey: "7", inCollection: propertyCollection)

            guard let relTransaction = transaction.ext("relationships") as? YapDatabaseRelationshipTransaction else {
                print("Error: relationship extension is required")
                return
            }

            let edge1 = YapDatabaseRelationshipEdge(name: "edgeName1",
                                                   sourceKey: "2", collection: ownerCollection,
                                                   destinationKey: "3", collection: propertyCollection,
                                                   nodeDeleteRules: .deleteDestinationIfSourceDeleted)
            relTransaction.add(edge1)

            let edge2 = YapDatabaseRelationshipEdge(name: "edgeName2",
                                                   sourceKey: "4", collection: ownerCollection,
                                                   destinationKey: "7", collection: propertyCollection,
                                                   nodeDeleteRules: .deleteDestinationIfSourceDeleted)
            relTransaction.add(edge2)
        }

        assert(expectedKeys: ["1", "2"], inCollection: rootCollection)
        assert(expectedKeys: ["2", "4"], inCollection: ownerCollection)
        assert(expectedKeys: ["3", "7"], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)

        // Remove one of the owner objects in the relation tree
        // NOTE This 'fixes' the problem introduced above by removing owner:2, which means
        //      that property:3 should also be deleted.
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.removeObject(forKey: "2", inCollection: ownerCollection)
        }

        assert(expectedKeys: ["1", "2"], inCollection: rootCollection)
        assert(expectedKeys: ["4"], inCollection: ownerCollection)
        assert(expectedKeys: ["7"], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)
        
        // Ressurect the unrelated object that was removed and create two new related objects
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.setObject("Unrelated 1", forKey: "1", inCollection: unrelatedCollection)
            
            transaction.setObject("Owner 10", forKey: "10", inCollection: ownerCollection)
            transaction.setObject("Property 11", forKey: "11", inCollection: propertyCollection)

            guard let relTransaction = transaction.ext("relationships") as? YapDatabaseRelationshipTransaction else {
                print("Error: relationship extension is required")
                return
            }

            let edge1 = YapDatabaseRelationshipEdge(name: "edgeName3",
                                                   sourceKey: "10", collection: ownerCollection,
                                                   destinationKey: "11", collection: propertyCollection,
                                                   nodeDeleteRules: .deleteDestinationIfSourceDeleted)
            relTransaction.add(edge1)
        }

        assert(expectedKeys: ["1", "2"], inCollection: rootCollection)
        assert(expectedKeys: ["4", "10"], inCollection: ownerCollection)
        assert(expectedKeys: ["7", "11"], inCollection: propertyCollection)
        assert(expectedKeys: ["1"], inCollection: unrelatedCollection)

        // Remove the resurrected object again
        YapDatabaseTests.connection?.readWrite { transaction in
            transaction.removeObject(forKey: "1", inCollection: unrelatedCollection)
        }

        assert(expectedKeys: ["1", "2"], inCollection: rootCollection)
        assert(expectedKeys: ["4", "10"], inCollection: ownerCollection)
        assert(expectedKeys: ["7", "11"], inCollection: propertyCollection)
        assert(expectedKeys: [], inCollection: unrelatedCollection)
    }

    func assert(expectedKeys: Array<String>, inCollection collection: String, line: UInt = #line) {
        YapDatabaseTests.connection?.read { transaction in
            let keys = transaction.allKeys(inCollection: collection)
            XCTAssertEqual(Set(keys), Set(expectedKeys), "keys in collection \(collection) not expected", line: line)
        }
    }
}
