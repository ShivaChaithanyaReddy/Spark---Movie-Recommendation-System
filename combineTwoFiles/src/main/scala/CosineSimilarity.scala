/* 
 * Object in scala for calculating cosine similarity
 * Shiva Chaithanya
 * More information: http://en.wikipedia.org/wiki/Cosine_similarity
 */

class CosineSimilarity() {
  
  /*
   * This method takes 2 equal length arrays of integers 
   * It returns a double representing similarity of the 2 arrays
   * 0.9925 would be 99.25% similar
   * (x dot y)/||X|| ||Y||
   */
  def cosineSimilarity(x: List[Double], y: List[Double]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
  
  /*
   * Return the dot product of the 2 arrays
   * e.g. (a[0]*b[0])+(a[1]*a[2])
   */
  def dotProduct(x: List[Double], y: List[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  
  /*
   * Return the magnitude of an array
   * We multiply each element, sum it, then square root the result.
   */
  def magnitude(x: List[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }
}
