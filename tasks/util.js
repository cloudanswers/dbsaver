function chunk(listToChunk, chunkSize) {
  let res = [[]];
  listToChunk.map((x) => {
    if (res[res.length - 1].length >= chunkSize) res.push([]);
    res[res.length - 1].push(x);
  });
  return res;
}

module.exports = { chunk };
