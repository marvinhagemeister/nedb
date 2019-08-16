import { uid } from "../util";

describe("customUtils", () => {
  describe("uid", () => {
    it("Generates a string of the expected length", () => {
      expect(uid(3).length).toEqual(3);
      expect(uid(16).length).toEqual(16);
      expect(uid(42).length).toEqual(42);
      expect(uid(1000).length).toEqual(1000);
    });

    // Very small probability of conflict
    it("Generated uids should not be the same", () => {
      expect(uid(56) !== uid(56)).toEqual(true);
    });
  });
});
