export type Query = Record<string, any>;
/**
 * SortQuery is { field: order }, field can use the dot-notation, order is 1
 * for ascending and -1 for descending
 */
export type SortQuery<K> = Record<keyof K, -1 | 0 | 1>;
