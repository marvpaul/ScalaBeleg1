package wordcount

import common._
import mapreduce.BasicOperations

class Processing {

	/** ********************************************************************************************
	  *
	  * Aufgabe 1
	  *
	  * ********************************************************************************************
	  */
	def getWords(line: String): List[String] = {
		/*
		 * Extracts all words in a line
		 *
		 * 1. Removes all characters which are not letters (A-Z or a-z)
		 * 2. Shifts all words to lower case
		 * 3. Extracts all words and put them into a list of strings
		 */
		if(line == ""){
			List()
		} else{
			line.replaceAll("[^A-Za-z ]", " ").replaceAll("  *", " ").toLowerCase.split(" ").filter(x => x != "").toList

		}
	}

	/**
	  * Extracts all Words from a List containing tupels consisting
	  * of a line number and a string
	  * The Words should be in the same order as they occur in the source document
	  *
	  * Hint: Use the flatMap function
	  * @param l a list with line number and the line as a string
	  * @return each word as a list elem like ["Hello", "nice", "text"]
	  */
	def getAllWords(l: List[(Int, String)]): List[String] = {


		l.flatMap(x => getWords(x._2))
	}

	/**
	  * Count the occurrences of each word in a given text
	  * @param l a list with words
	  * @return a list in following format: [["hello", 2], ["no", 3], ["yes", 1]]
	  */
	def countTheWords(l: List[String]): List[(String, Int)] = {

		/*
		 *  Gets a list of words and counts the occurences of the individual words
		 */
		l
			.groupBy(w => w)
			.map(x => (x._1, x._2.count(t => true)))
			.toList
	}

	/** ********************************************************************************************
	  *
	  * Aufgabe 2
	  *
	  * ********************************************************************************************
	  */

	def mapReduce[String, Tuple, MapStringInt](mapFun: (String => Tuple),
	                                           redFun: (MapStringInt, Tuple) => MapStringInt,
	                                           base: MapStringInt,
	                                           l: List[String]): MapStringInt =

		l.map(mapFun).foldLeft(base)(redFun)

	def countTheWordsMR(l: List[String]): List[(String, Int)] = {
		mapReduce[String, (String, Int), Map[String, Int]](
			word => (word, 1),
			(mapFromWordToCounter, tupleWithWordAndCount) => {
				//Update the map for current word by increment the counter (++)
				mapFromWordToCounter.updated(tupleWithWordAndCount._1, 1 + mapFromWordToCounter.getOrElse(tupleWithWordAndCount._1, 0))
			},
			Map[String, Int](),
			l
		).toList
	}


	/** ********************************************************************************************
	  *
	  * Aufgabe 3
	  *
	  * ********************************************************************************************
	  */

	/**
	  *
	  * @param list_with_linenr_and_linetext a list with a line nr and all words as a string
	  * @return a list of tuples containing line nr and 1 word
	  */
	def getAllWordsWithIndex(list_with_linenr_and_linetext: List[(Int, String)]): List[(Int, String)] = {
		/*
	 * Extracts all Words from a List containing tupels consisting
	 * of a line number and a string
	 */
		var words_in_line_with_number = list_with_linenr_and_linetext
			//Map to list with all words in a row
			.flatMap(linetext_and_linenr => getWords(linetext_and_linenr._2)
				//Map to tuple of (linenr, word)
				.map(list_with_all_words_in_one_line => (linetext_and_linenr._1, list_with_all_words_in_one_line)))

		//Make a simple list
		words_in_line_with_number.foldLeft(List[(Int, String)]())((list, line) => List(line) ++ list)
	}


	/**
	  * Create inverse index of a given list with (line_nr, word)
	  * Inverse index is in following format:
	  * word -> line_nr
	  * Example: "Hello" -> [1, 10, 12]
	  * @param l a given list with tuples --> (line_nr, word)
	  * @return the inverse index
	  */
	def createInverseIndex(l: List[(Int, String)]): Map[String, List[Int]] = {

		l.foldLeft(Map[String, List[Int]]())((list, word_with_linenr) => {
				//Add line number to existing entry in map or create an empty list and append
				list.updated(word_with_linenr._2, word_with_linenr._1 :: list.getOrElse(word_with_linenr._2, List()))
		})
	}

	/**
	  * Get all lines where all words words are in
	  * @param words a given word list
	  * @param invInd the inverse index of a text
	  * @return a list with all linenr where words are in
	  */
	def andConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {

		val lines_where_word_is_in = getLinesWhereAWordIsIn(words, invInd)

		//Check if there's any word which occurs in all lines
		if(lines_where_word_is_in.length == words.length){
			lines_where_word_is_in.foldLeft(List[Int]())((list, new_line_occurences) => {
				//List is empty, just add the occurences of the word to list
				if (list.isEmpty) {
					new_line_occurences ++  list //Case the list is empty add the lines where a word is in to list
				} else {
					//Case some lines are already in list, check if the new word occurs in the same lines
					val new_occurences_plus_old = new_line_occurences ++ list
					val all_line_numbers_once = List.concat(new_line_occurences, list).distinct
					//Return a list with elements which occurs in the old analyzed lines and in the new one
					new_occurences_plus_old.diff(all_line_numbers_once).distinct
				}
			})
		} else{
			List()
		}
	}

	/**
	  * Get all lines where a certain words occurs in
	  * @param words a given list of words e.g. ["Hallo", "Tschüss"]
	  * @param invInd the inv index of a text
	  * @return a list with all lines numbers a word occurs in,e.g. [[1, 2], [10]]
	  */
	def getLinesWhereAWordIsIn(words: List[String], invInd: Map[String, List[Int]]): List[List[Int]] = {
		words.foldLeft(List[List[Int]]())((list, word_to_search_for) => invInd.get(word_to_search_for) match {
			case Some(elem) => {
				invInd.get(word_to_search_for).head :: list //Add all lines where the current word occures in
			}
			case None => list
		})
	}

	/**
	  * Get all lines where at least one of the given words occurs in
	  * @param words a list with given words
	  * @param invInd the inverse index of a text
	  * @return a list with line nr where at least one word occurs in
	  */
	def orConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {
		val lines_where_word_is_in = getLinesWhereAWordIsIn(words, invInd)
		lines_where_word_is_in.foldLeft(List[Int]())((list, lines) => {
			List.concat(list, lines).distinct //Add each line where one word occurs to list, distinct makes each line number unique
		})
	}
}


object Processing {

	def getData(filename: String): List[(Int, String)] = {

		val url = getClass.getResource("/" + filename).getPath
		val src = scala.io.Source.fromFile(url)
		val iter = src.getLines()
		var c = -1
		val result = (for (row <- iter) yield {
			c = c + 1; (c, row)
		}).toList
		src.close()
		result
	}
}