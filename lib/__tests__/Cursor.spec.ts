import { exists } from "nicer-fs";
import { promisify } from "util";
import * as fs from "fs";
import * as path from "path";
import Datastore from "../datastore";
import Cursor from "../Cursor";
import * as model from "../model";
import { ensureDirectoryExists } from "../persistence";

const testDb = "workspace/test.db";

describe("Cursor", () => {
  let d: Datastore;

  beforeEach(async () => {
    d = new Datastore({ filename: testDb });
    expect(d.filename).toEqual(testDb);
    expect(d.inMemoryOnly).toEqual(false);

    await ensureDirectoryExists(path.dirname(testDb));
    const res = await exists(testDb);
    if (res) await promisify(fs.unlink)(testDb);
    await d.loadDatabase();
    expect(d.getAllData().length).toEqual(0);
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
      let docs = await new Cursor(d).exec();
      expect(docs.length).toEqual(5);

      expect(docs.filter(doc => doc.age === 5)[0].age).toEqual(5);
      expect(docs.filter(doc => doc.age === 57)[0].age).toEqual(57);
      expect(docs.filter(doc => doc.age === 52)[0].age).toEqual(52);
      expect(docs.filter(doc => doc.age === 23)[0].age).toEqual(23);
      expect(docs.filter(doc => doc.age === 89)[0].age).toEqual(89);

      docs = await new Cursor(d, { age: { $gt: 23 } }).exec();
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
      const docs = await new Cursor(d).limit(3).exec();
      expect(docs.length).toEqual(3);
    });

    it("With a skip", async () => {
      const docs = await new Cursor(d).skip(2).exec();
      expect(docs.length).toEqual(3);
    });

    it("With a limit and a skip and method chaining", async () => {
      const docs = await new Cursor(d).limit(4).skip(3);
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
      docs = await new Cursor(d).sort({ age: 1 });
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
      let docs = await new Cursor(d)
        .sort({ age: 1 })
        .limit(1)
        .skip(2)
        .exec();
      expect(docs.length).toEqual(1);
      expect(docs[0].age).toEqual(52);

      docs = await new Cursor(d)
        .sort({ age: 1 })
        .limit(3)
        .skip(1)
        .exec();
      expect(docs.map(x => x.age)).toEqual([23, 52, 57]);

      docs = await new Cursor(d)
        .sort({ age: -1 })
        .limit(2)
        .skip(2)
        .exec();
      expect(docs.map(x => x.age)).toEqual([52, 23]);
    });

    it("Using too big a limit and a skip with sort", async () => {
      const docs = await new Cursor(d)
        .sort({ age: 1 })
        .limit(8)
        .skip(2)
        .exec();
      expect(docs).toEqual([52, 57, 89]);
    });

    it("Using too big a skip with sort should return no result", async () => {
      let docs = await new Cursor(d)
        .sort({ age: 1 })
        .skip(5)
        .exec();
      expect(docs).toEqual([]);

      docs = await new Cursor(d)
        .sort({ age: 1 })
        .skip(7)
        .exec();
      expect(docs).toEqual([]);

      docs = await new Cursor(d)
        .sort({ age: 1 })
        .limit(3)
        .skip(7)
        .exec();
      expect(docs).toEqual([]);

      docs = await new Cursor(d)
        .sort({ age: 1 })
        .limit(6)
        .skip(7)
        .exec();
      expect(docs).toEqual([]);
    });

    it("Sorting strings", async () => {
      await d.remove({}, { multi: true });
      await d.insert({ name: "jako" });
      await d.insert({ name: "jakeb" });
      await d.insert({ name: "sue" });

      let docs = await new Cursor(d, {}).sort({ name: 1 }).exec();
      expect(docs.map(x => x.name)).toEqual(["jakeb", "jako", "sue"]);

      docs = await new Cursor(d, {}).sort({ name: -1 }).exec();
      expect(docs.map(x => x.name)).toEqual(["sue", "jako", "jakeb"]);
    });

    it("Sorting nested fields with dates", async () => {
      await d.remove({}, { multi: true });
      const doc1 = await d.insert({ event: { recorded: new Date(400) } });
      const doc2 = await d.insert({
        event: { recorded: new Date(60000) },
      });
      const doc3 = await d.insert({ event: { recorded: new Date(32) } });

      let docs = await new Cursor(d, {}).sort({ "event.recorded": 1 }).exec();
      expect(docs.map(x => x._id)).toEqual([doc3._id, doc1._id, doc2._id]);

      docs = await new Cursor(d, {}).sort({ "event.recorded": -1 }).exec();
      expect(docs.map(x => x._id)).toEqual([doc2._id, doc1._id, doc3._id]);
    });

    it("Sorting when some fields are undefined", async () => {
      await d.remove({}, { multi: true });
      await d.insert({ name: "jako", other: 2 });
      await d.insert({ name: "jakeb", other: 3 });
      await d.insert({ name: "sue" });
      await d.insert({ name: "henry", other: 4 });

      let docs = await new Cursor(d, {}).sort({ other: 1 }).exec();
      expect(docs).toEqual([
        { name: "sue", other: undefined },
        { name: "jako", other: 2 },
        { name: "jakeb", other: 3 },
        { name: "henry", other: 4 },
      ]);

      docs = await new Cursor(d, {
        name: { $in: ["suzy", "jakeb", "jako"] },
      })
        .sort({ other: -1 })
        .exec();
      expect(docs).toEqual([
        { name: "jakeb", other: 3 },
        { name: "jako", other: 2 },
      ]);
    });

    it("Sorting when all fields are undefined", async () => {
      await d.remove({}, { multi: true });
      await d.insert({ name: "jako" });
      await d.insert({ name: "jakeb" });
      await d.insert({ name: "sue" });

      let docs = await new Cursor(d, {}).sort({ other: 1 }).exec();
      expect(docs.length).toEqual(3);

      docs = await new Cursor(d, {
        name: { $in: ["sue", "jakeb", "jakob"] },
      })
        .sort({ other: -1 })
        .exec();
      expect(docs.length).toEqual(2);
    });

    it("Multiple consecutive sorts", async () => {
      await d.remove({}, { multi: true });
      await d.insert({ name: "jako", age: 43, nid: 1 });
      await d.insert({ name: "jakeb", age: 43, nid: 2 });
      await d.insert({ name: "sue", age: 12, nid: 3 });
      await d.insert({ name: "zoe", age: 23, nid: 4 });
      await d.insert({ name: "jako", age: 35, nid: 5 });

      let docs = await new Cursor(d, {}).sort({ name: 1, age: -1 }).exec();
      expect(docs.map(doc => doc.nid)).toEqual([2, 1, 5, 3, 4]);

      docs = await new Cursor(d, {}).sort({ name: 1, age: 1 }).exec();
      expect(docs.map(doc => doc.nid)).toEqual([2, 5, 1, 3, 4]);

      docs = await new Cursor(d, {}).sort({ age: 1, name: 1 }).exec();
      expect(docs.map(doc => doc.nid)).toEqual([3, 4, 5, 2, 1]);

      docs = await new Cursor(d, {}).sort({ age: 1, name: -1 }).exec();
      expect(docs.map(doc => doc.nid)).toEqual([3, 4, 5, 1, 2]);
    });

    it("Similar data, multiple consecutive sorts", async () => {
      const companies = ["acme", "milkman", "zoinks"];
      const entities: any[] = [];

      await d.remove({}, { multi: true });

      let id = 1;
      for (let i = 0; i < companies.length; i++) {
        for (let j = 5; j <= 100; j += 5) {
          entities.push({
            company: companies[i],
            cost: j,
            nid: id,
          });
          id++;
        }
      }

      await Promise.all(entities.map(x => d.insert(x)));

      const docs = await new Cursor(d, {}).sort({ company: 1, cost: 1 }).exec();
      expect(docs.length).toEqual(60);

      for (let i = 0; i < docs.length; i++) {
        expect(docs[i].nid).toEqual(i + 1);
      }
    });
  });

  describe("Projections", () => {
    let doc1: any, doc2: any, doc3: any, doc4: any, doc0: any;

    beforeEach(async () => {
      // We don't know the order in which docs wil be inserted but we ensure correctness by testing both sort orders
      doc0 = await d.insert({
        age: 5,
        name: "Jo",
        planet: "B",
        toys: { bebe: true, ballon: "much" },
      });
      doc1 = await d.insert({
        age: 57,
        name: "Louis",
        planet: "R",
        toys: { ballon: "yeah", bebe: false },
      });
      doc2 = await d.insert({
        age: 52,
        name: "Grafitti",
        planet: "C",
        toys: { bebe: "kind of" },
      });
      doc3 = await d.insert({ age: 23, name: "LM", planet: "S" });
      doc4 = await d.insert({ age: 89, planet: "Earth" });
    });

    it("Takes all results if no projection or empty object given", async () => {
      const cursor = new Cursor(d, {});
      let docs = await cursor.sort({ age: 1 }).exec();
      expect(docs).toEqual([doc0, doc3, doc2, doc1, doc4]);

      docs = await cursor.projection({}).exec();
      expect(docs).toEqual([doc0, doc3, doc2, doc1, doc4]);
    });

    it("Can take only the expected fields", async () => {
      let cursor = new Cursor(d, {})
        .sort({ age: 1 })
        .projection({ age: 1, name: 1 });
      let docs = await cursor.exec();
      // Takes the _id by default
      expect(docs).toEqual([
        { age: 5, name: "Jo", _id: doc0._id },
        { age: 23, name: "LM", _id: doc3._id },
        { age: 52, name: "Grafitti", _id: doc2._id },
        { age: 57, name: "Louis", _id: doc1._id },
        { age: 89, _id: doc4._id },
      ]);

      docs = await cursor.projection({ age: 1, name: 1, _id: 0 }).exec();
      expect(docs).toEqual([
        { age: 5, name: "Jo" },
        { age: 23, name: "LM" },
        { age: 52, name: "Grafitti" },
        { age: 57, name: "Louis" },
        { age: 89 },
      ]);
    });

    it("Can omit only the expected fields", async () => {
      let cursor = new Cursor(d, {})
        .sort({ age: 1 })
        .projection({ age: 0, name: 0 });

      let docs = await cursor.exec();
      // Takes the _id by default
      expect(docs).toEqual([
        {
          planet: "B",
          _id: doc0._id,
          toys: { bebe: true, ballon: "much" },
        },
        { planet: "S", _id: doc3._id },
        {
          planet: "C",
          _id: doc2._id,
          toys: { bebe: "kind of" },
        },
        {
          planet: "R",
          _id: doc1._id,
          toys: { bebe: false, ballon: "yeah" },
        },
        { planet: "Earth", _id: doc4._id },
      ]);

      docs = await cursor.projection({ age: 0, name: 0, _id: 0 }).exec();
      expect(docs).toEqual([
        {
          planet: "B",
          toys: { bebe: true, ballon: "much" },
        },
        { planet: "S" },
        { planet: "C", toys: { bebe: "kind of" } },
        {
          planet: "R",
          toys: { bebe: false, ballon: "yeah" },
        },
        { planet: "Earth" },
      ]);
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
