import Datastore from "../datastore";
import Cursor from "../Cursor";

var should = require("chai").should(),
  assert = require("chai").assert,
  testDb = "workspace/test.db",
  fs = require("fs"),
  path = require("path"),
  _ = require("underscore"),
  async = require("async"),
  model = require("../lib/model"),
  Persistence = require("../lib/persistence");

describe("Cursor", () => {
  let d: Datastore;

  beforeEach(async () => {
    d = new Datastore({ filename: testDb });
    expect(d.filename).toEqual(testDb);
    expect(d.inMemoryOnly).toEqual(false);

    async.waterfall(
      [
        function(cb) {
          Persistence.ensureDirectoryExists(path.dirname(testDb), function() {
            fs.exists(testDb, function(exists) {
              if (exists) {
                fs.unlink(testDb, cb);
              } else {
                return cb();
              }
            });
          });
        },
        function(cb) {
          d.loadDatabase(function(err) {
            assert.isNull(err);
            d.getAllData().length.should.equal(0);
            return cb();
          });
        },
      ],
      done,
    );
  });

  describe("Without sorting", () => {
    beforeEach(async () => {
      await d.insert({ age: 5 });
      await d.insert({ age: 57 });
      await d.insert({ age: 52 });
      await d.insert({ age: 23 });
      await d.insert({ age: 89 });
    });

    it("Without query, an empty query or a simple query and no skip or limit", async () => {
      let cursor = new Cursor(d);
      let docs = await cursor.exec();
      expect(docs.length).toEqual(5);

      expect(docs.filter(doc => doc.age === 5)[0].age).toEqual(5);
      expect(docs.filter(doc => doc.age === 57)[0].age).toEqual(57);
      expect(docs.filter(doc => doc.age === 52)[0].age).toEqual(52);
      expect(docs.filter(doc => doc.age === 23)[0].age).toEqual(23);
      expect(docs.filter(doc => doc.age === 89)[0].age).toEqual(89);

      cursor = new Cursor(d, { age: { $gt: 23 } });
      docs = await cursor.exec();
      expect(docs.length).toEqual(3);
      expect(docs.filter(doc => doc.age === 57)[0].age).toEqual(57);
      expect(docs.filter(doc => doc.age === 52)[0].age).toEqual(52);
      expect(docs.filter(doc => doc.age === 89)[0].age).toEqual(89);
    });

    it("With an empty collection", async () => {
      await d.remove({}, { multi: true });
      const docs = new Cursor(d).exec();
      expect(docs.length).toEqual(0);
    });

    it("With a limit", async () => {
      var cursor = new Cursor(d);
      cursor.limit(3);
      const docs = await cursor.exec();
      expect(docs.length).toEqual(3);
    });

    it("With a skip", async () => {
      var cursor = new Cursor(d);
      const docs = await cursor.skip(2).exec();
      expect(docs.length).toEqual(3);
    });

    it("With a limit and a skip and method chaining", async () => {
      var cursor = new Cursor(d);
      const docs = await cursor.limit(4).skip(3);
      // Only way to know that the right number of results was skipped is if
      // limit + skip > number of results
      expect(docs.length).toEqual(2);
    });
  });

  describe("Sorting of the results", () => {
    beforeEach(async () => {
      // We don't know the order in which docs wil be inserted but we ensure correctness by testing both sort orders
      await d.insert({ age: 5 });
      await d.insert({ age: 57 });
      await d.insert({ age: 52 });
      await d.insert({ age: 23 });
      await d.insert({ age: 89 });
    });

    it("Using one sort", async () => {
      var i;

      const cursor = new Cursor(d, {});
      let docs = await cursor.sort({ age: 1 }).exec();
      // Results are in ascending order
      for (i = 0; i < docs.length - 1; i += 1) {
        expect(docs[i].age < docs[i + 1].age).toEqual(true);
      }

      cursor.sort({ age: -1 });
      docs = await cursor.exec();
      // Results are in descending order
      for (i = 0; i < docs.length - 1; i += 1) {
        expect(docs[i].age > docs[i + 1].age).toEqual(true);
      }
    });

    it("Sorting strings with custom string comparison function", async () => {
      var db = new Datastore({
        inMemoryOnly: true,
        autoload: true,
        compareStrings(a: any, b: any) {
          return a.length - b.length;
        },
      });

      await db.insert({ name: "alpha" });
      await db.insert({ name: "charlie" });
      await db.insert({ name: "zulu" });

      let docs = await db
        .find({})
        .sort({ name: 1 })
        .exec();

      expect(docs.map(x => x.name)).toEqual(["zulu", "alpha", "charlie"]);

      delete db.compareStrings;
      docs = db
        .find({})
        .sort({ name: 1 })
        .exec();

      expect(docs.map(x => x.name)).toEqual(["alpha", "charlie", "zulu"]);
    });

    it("With an empty collection", async () => {
      await d.remove({}, { multi: true });
      var cursor = new Cursor(d).sort({ age: 1 });
      const docs = await cursor.exec();

      expect(docs.length).toEqual(0);
    });

    it("Ability to chain sorting and exec", async () => {
      let docs = await new Cursor(d).sort({ age: 1 }).exec();
      // Results are in ascending order
      for (let i = 0; i < docs.length - 1; i += 1) {
        expect(docs[i].age < docs[i + 1].age).toEqual(true);
      }

      docs = await new Cursor(d).sort({ age: -1 }).exec();
      // Results are in descending order
      for (let i = 0; i < docs.length - 1; i += 1) {
        expect(docs[i].age > docs[i + 1].age).toEqual(true);
      }
    });

    it("Using limit and sort", async () => {
      let docs = await new Cursor(d)
        .sort({ age: 1 })
        .limit(3)
        .exec();

      expect(docs).toEqual([5, 23, 52]);

      docs = await new Cursor(d)
        .sort({ age: -1 })
        .limit(2)
        .exec();

      expect(docs).toEqual([2, 89]);
    });

    it("Using a limit higher than total number of docs shouldnt cause an error", async () => {
      const docs = await new Cursor(d)
        .sort({ age: 1 })
        .limit(7)
        .exec();

      expect(docs).toEqual([5, 23, 52, 57, 89]);
    });

    it("Using limit and skip with sort", async () => {
      var i;
      async.waterfall(
        [
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: 1 })
              .limit(1)
              .skip(2)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(1);
                docs[0].age.should.equal(52);
                cb();
              });
          },
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: 1 })
              .limit(3)
              .skip(1)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(3);
                docs[0].age.should.equal(23);
                docs[1].age.should.equal(52);
                docs[2].age.should.equal(57);
                cb();
              });
          },
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: -1 })
              .limit(2)
              .skip(2)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(2);
                docs[0].age.should.equal(52);
                docs[1].age.should.equal(23);
                cb();
              });
          },
        ],
        done,
      );
    });

    it("Using too big a limit and a skip with sort", async () => {
      var i;
      async.waterfall(
        [
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: 1 })
              .limit(8)
              .skip(2)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(3);
                docs[0].age.should.equal(52);
                docs[1].age.should.equal(57);
                docs[2].age.should.equal(89);
                cb();
              });
          },
        ],
        done,
      );
    });

    it("Using too big a skip with sort should return no result", async () => {
      var i;
      async.waterfall(
        [
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: 1 })
              .skip(5)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(0);
                cb();
              });
          },
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: 1 })
              .skip(7)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(0);
                cb();
              });
          },
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: 1 })
              .limit(3)
              .skip(7)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(0);
                cb();
              });
          },
          function(cb) {
            var cursor = new Cursor(d);
            cursor
              .sort({ age: 1 })
              .limit(6)
              .skip(7)
              .exec(function(err, docs) {
                assert.isNull(err);
                docs.length.should.equal(0);
                cb();
              });
          },
        ],
        done,
      );
    });

    it("Sorting strings", async () => {
      async.waterfall(
        [
          function(cb) {
            d.remove({}, { multi: true }, function(err) {
              if (err) {
                return cb(err);
              }

              d.insert({ name: "jako" }, function() {
                d.insert({ name: "jakeb" }, function() {
                  d.insert({ name: "sue" }, function() {
                    return cb();
                  });
                });
              });
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ name: 1 }).exec(function(err, docs) {
              docs.length.should.equal(3);
              docs[0].name.should.equal("jakeb");
              docs[1].name.should.equal("jako");
              docs[2].name.should.equal("sue");
              return cb();
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ name: -1 }).exec(function(err, docs) {
              docs.length.should.equal(3);
              docs[0].name.should.equal("sue");
              docs[1].name.should.equal("jako");
              docs[2].name.should.equal("jakeb");
              return cb();
            });
          },
        ],
        done,
      );
    });

    it("Sorting nested fields with dates", async () => {
      var doc1, doc2, doc3;

      async.waterfall(
        [
          function(cb) {
            d.remove({}, { multi: true }, function(err) {
              if (err) {
                return cb(err);
              }

              d.insert({ event: { recorded: new Date(400) } }, function(
                err,
                _doc1,
              ) {
                doc1 = _doc1;
                d.insert({ event: { recorded: new Date(60000) } }, function(
                  err,
                  _doc2,
                ) {
                  doc2 = _doc2;
                  d.insert({ event: { recorded: new Date(32) } }, function(
                    err,
                    _doc3,
                  ) {
                    doc3 = _doc3;
                    return cb();
                  });
                });
              });
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ "event.recorded": 1 }).exec(function(err, docs) {
              docs.length.should.equal(3);
              docs[0]._id.should.equal(doc3._id);
              docs[1]._id.should.equal(doc1._id);
              docs[2]._id.should.equal(doc2._id);
              return cb();
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ "event.recorded": -1 }).exec(function(err, docs) {
              docs.length.should.equal(3);
              docs[0]._id.should.equal(doc2._id);
              docs[1]._id.should.equal(doc1._id);
              docs[2]._id.should.equal(doc3._id);
              return cb();
            });
          },
        ],
        done,
      );
    });

    it("Sorting when some fields are undefined", async () => {
      async.waterfall(
        [
          function(cb) {
            d.remove({}, { multi: true }, function(err) {
              if (err) {
                return cb(err);
              }

              d.insert({ name: "jako", other: 2 }, function() {
                d.insert({ name: "jakeb", other: 3 }, function() {
                  d.insert({ name: "sue" }, function() {
                    d.insert({ name: "henry", other: 4 }, function() {
                      return cb();
                    });
                  });
                });
              });
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ other: 1 }).exec(function(err, docs) {
              docs.length.should.equal(4);
              docs[0].name.should.equal("sue");
              assert.isUndefined(docs[0].other);
              docs[1].name.should.equal("jako");
              docs[1].other.should.equal(2);
              docs[2].name.should.equal("jakeb");
              docs[2].other.should.equal(3);
              docs[3].name.should.equal("henry");
              docs[3].other.should.equal(4);
              return cb();
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {
              name: { $in: ["suzy", "jakeb", "jako"] },
            });
            cursor.sort({ other: -1 }).exec(function(err, docs) {
              docs.length.should.equal(2);
              docs[0].name.should.equal("jakeb");
              docs[0].other.should.equal(3);
              docs[1].name.should.equal("jako");
              docs[1].other.should.equal(2);
              return cb();
            });
          },
        ],
        done,
      );
    });

    it("Sorting when all fields are undefined", async () => {
      async.waterfall(
        [
          function(cb) {
            d.remove({}, { multi: true }, function(err) {
              if (err) {
                return cb(err);
              }

              d.insert({ name: "jako" }, function() {
                d.insert({ name: "jakeb" }, function() {
                  d.insert({ name: "sue" }, function() {
                    return cb();
                  });
                });
              });
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ other: 1 }).exec(function(err, docs) {
              docs.length.should.equal(3);
              return cb();
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {
              name: { $in: ["sue", "jakeb", "jakob"] },
            });
            cursor.sort({ other: -1 }).exec(function(err, docs) {
              docs.length.should.equal(2);
              return cb();
            });
          },
        ],
        done,
      );
    });

    it("Multiple consecutive sorts", async () => {
      async.waterfall(
        [
          function(cb) {
            d.remove({}, { multi: true }, function(err) {
              if (err) {
                return cb(err);
              }

              d.insert({ name: "jako", age: 43, nid: 1 }, function() {
                d.insert({ name: "jakeb", age: 43, nid: 2 }, function() {
                  d.insert({ name: "sue", age: 12, nid: 3 }, function() {
                    d.insert({ name: "zoe", age: 23, nid: 4 }, function() {
                      d.insert({ name: "jako", age: 35, nid: 5 }, function() {
                        return cb();
                      });
                    });
                  });
                });
              });
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ name: 1, age: -1 }).exec(function(err, docs) {
              docs.length.should.equal(5);

              docs[0].nid.should.equal(2);
              docs[1].nid.should.equal(1);
              docs[2].nid.should.equal(5);
              docs[3].nid.should.equal(3);
              docs[4].nid.should.equal(4);
              return cb();
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ name: 1, age: 1 }).exec(function(err, docs) {
              docs.length.should.equal(5);

              docs[0].nid.should.equal(2);
              docs[1].nid.should.equal(5);
              docs[2].nid.should.equal(1);
              docs[3].nid.should.equal(3);
              docs[4].nid.should.equal(4);
              return cb();
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ age: 1, name: 1 }).exec(function(err, docs) {
              docs.length.should.equal(5);

              docs[0].nid.should.equal(3);
              docs[1].nid.should.equal(4);
              docs[2].nid.should.equal(5);
              docs[3].nid.should.equal(2);
              docs[4].nid.should.equal(1);
              return cb();
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ age: 1, name: -1 }).exec(function(err, docs) {
              docs.length.should.equal(5);

              docs[0].nid.should.equal(3);
              docs[1].nid.should.equal(4);
              docs[2].nid.should.equal(5);
              docs[3].nid.should.equal(1);
              docs[4].nid.should.equal(2);
              return cb();
            });
          },
        ],
        done,
      );
    });

    it("Similar data, multiple consecutive sorts", async () => {
      var i,
        j,
        id,
        companies = ["acme", "milkman", "zoinks"],
        entities = [];

      async.waterfall(
        [
          function(cb) {
            d.remove({}, { multi: true }, function(err) {
              if (err) {
                return cb(err);
              }

              id = 1;
              for (i = 0; i < companies.length; i++) {
                for (j = 5; j <= 100; j += 5) {
                  entities.push({
                    company: companies[i],
                    cost: j,
                    nid: id,
                  });
                  id++;
                }
              }

              async.each(
                entities,
                function(entity, callback) {
                  d.insert(entity, function() {
                    callback();
                  });
                },
                function(err) {
                  return cb();
                },
              );
            });
          },
          function(cb) {
            var cursor = new Cursor(d, {});
            cursor.sort({ company: 1, cost: 1 }).exec(function(err, docs) {
              docs.length.should.equal(60);

              for (var i = 0; i < docs.length; i++) {
                docs[i].nid.should.equal(i + 1);
              }
              return cb();
            });
          },
        ],
        done,
      );
    });
  }); // ===== End of 'Sorting' =====

  describe("Projections", function() {
    var doc1, doc2, doc3, doc4, doc0;

    beforeEach(async () => {
      // We don't know the order in which docs wil be inserted but we ensure correctness by testing both sort orders
      d.insert(
        {
          age: 5,
          name: "Jo",
          planet: "B",
          toys: { bebe: true, ballon: "much" },
        },
        function(err, _doc0) {
          doc0 = _doc0;
          d.insert(
            {
              age: 57,
              name: "Louis",
              planet: "R",
              toys: { ballon: "yeah", bebe: false },
            },
            function(err, _doc1) {
              doc1 = _doc1;
              d.insert(
                {
                  age: 52,
                  name: "Grafitti",
                  planet: "C",
                  toys: { bebe: "kind of" },
                },
                function(err, _doc2) {
                  doc2 = _doc2;
                  d.insert({ age: 23, name: "LM", planet: "S" }, function(
                    err,
                    _doc3,
                  ) {
                    doc3 = _doc3;
                    d.insert({ age: 89, planet: "Earth" }, function(
                      err,
                      _doc4,
                    ) {
                      doc4 = _doc4;
                      return done();
                    });
                  });
                },
              );
            },
          );
        },
      );
    });

    it("Takes all results if no projection or empty object given", async () => {
      var cursor = new Cursor(d, {});
      cursor.sort({ age: 1 }); // For easier finding
      cursor.exec(function(err, docs) {
        assert.isNull(err);
        docs.length.should.equal(5);
        assert.deepEqual(docs[0], doc0);
        assert.deepEqual(docs[1], doc3);
        assert.deepEqual(docs[2], doc2);
        assert.deepEqual(docs[3], doc1);
        assert.deepEqual(docs[4], doc4);

        cursor.projection({});
        cursor.exec(function(err, docs) {
          assert.isNull(err);
          docs.length.should.equal(5);
          assert.deepEqual(docs[0], doc0);
          assert.deepEqual(docs[1], doc3);
          assert.deepEqual(docs[2], doc2);
          assert.deepEqual(docs[3], doc1);
          assert.deepEqual(docs[4], doc4);

          done();
        });
      });
    });

    it("Can take only the expected fields", async () => {
      var cursor = new Cursor(d, {});
      cursor.sort({ age: 1 }); // For easier finding
      cursor.projection({ age: 1, name: 1 });
      cursor.exec(function(err, docs) {
        assert.isNull(err);
        docs.length.should.equal(5);
        // Takes the _id by default
        assert.deepEqual(docs[0], { age: 5, name: "Jo", _id: doc0._id });
        assert.deepEqual(docs[1], { age: 23, name: "LM", _id: doc3._id });
        assert.deepEqual(docs[2], { age: 52, name: "Grafitti", _id: doc2._id });
        assert.deepEqual(docs[3], { age: 57, name: "Louis", _id: doc1._id });
        assert.deepEqual(docs[4], { age: 89, _id: doc4._id }); // No problems if one field to take doesn't exist

        cursor.projection({ age: 1, name: 1, _id: 0 });
        cursor.exec(function(err, docs) {
          assert.isNull(err);
          docs.length.should.equal(5);
          assert.deepEqual(docs[0], { age: 5, name: "Jo" });
          assert.deepEqual(docs[1], { age: 23, name: "LM" });
          assert.deepEqual(docs[2], { age: 52, name: "Grafitti" });
          assert.deepEqual(docs[3], { age: 57, name: "Louis" });
          assert.deepEqual(docs[4], { age: 89 }); // No problems if one field to take doesn't exist

          done();
        });
      });
    });

    it("Can omit only the expected fields", async () => {
      var cursor = new Cursor(d, {});
      cursor.sort({ age: 1 }); // For easier finding
      cursor.projection({ age: 0, name: 0 });
      cursor.exec(function(err, docs) {
        assert.isNull(err);
        docs.length.should.equal(5);
        // Takes the _id by default
        assert.deepEqual(docs[0], {
          planet: "B",
          _id: doc0._id,
          toys: { bebe: true, ballon: "much" },
        });
        assert.deepEqual(docs[1], { planet: "S", _id: doc3._id });
        assert.deepEqual(docs[2], {
          planet: "C",
          _id: doc2._id,
          toys: { bebe: "kind of" },
        });
        assert.deepEqual(docs[3], {
          planet: "R",
          _id: doc1._id,
          toys: { bebe: false, ballon: "yeah" },
        });
        assert.deepEqual(docs[4], { planet: "Earth", _id: doc4._id });

        cursor.projection({ age: 0, name: 0, _id: 0 });
        cursor.exec(function(err, docs) {
          assert.isNull(err);
          docs.length.should.equal(5);
          assert.deepEqual(docs[0], {
            planet: "B",
            toys: { bebe: true, ballon: "much" },
          });
          assert.deepEqual(docs[1], { planet: "S" });
          assert.deepEqual(docs[2], { planet: "C", toys: { bebe: "kind of" } });
          assert.deepEqual(docs[3], {
            planet: "R",
            toys: { bebe: false, ballon: "yeah" },
          });
          assert.deepEqual(docs[4], { planet: "Earth" });

          done();
        });
      });
    });

    it("Cannot use both modes except for _id", async () => {
      const cursor = new Cursor(d, {})
        .sort({ age: 1 })
        .projection({ age: 1, name: 0 });
      let docs = cursor.exec();
      expect(docs).toEqual(undefined);

      cursor.projection({ age: 1, _id: 0 });
      docs = await cursor.exec();
      expect(docs).toEqual([
        { age: 5 },
        { age: 23 },
        { age: 52 },
        { age: 57 },
        { age: 89 },
      ]);

      cursor.projection({ age: 0, toys: 0, planet: 0, _id: 1 });
      docs = await cursor.exec();

      expect(docs).toEqual([
        { name: "Jo", _id: doc0._id },
        { name: "LM", _id: doc3._id },
        { name: "Grafitti", _id: doc2._id },
        { name: "Louis", _id: doc1._id },
        { _id: doc4._id },
      ]);
    });

    it("Projections on embedded documents - omit type", async () => {
      const docs = await new Cursor(d, {})
        .sort({ age: 1 })
        .projection({ name: 0, planet: 0, "toys.bebe": 0, _id: 0 })
        .exec();

      expect(docs).toEqual([
        { age: 5, toys: { ballon: "much" } },
        { age: 23 },
        { age: 52, toys: {} },
        { age: 57, toys: { ballon: "yeah" } },
        { age: 89 },
      ]);
    });

    it("Projections on embedded documents - pick type", async () => {
      const docs = await new Cursor(d, {})
        .sort({ age: 1 })
        .projection({ name: 1, "toys.ballon": 1, _id: 0 })
        .exec();

      expect(docs).toEqual([
        { name: "Jo", toys: { ballon: "much" } },
        { name: "LM" },
        { name: "Grafitti" },
        { name: "Louis", toys: { ballon: "yeah" } },
        {},
      ]);
    });
  });
});
