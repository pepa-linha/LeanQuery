<?php

namespace LeanQuery;

class ObjectHelpers
{
	/**
	 * Finds the best suggestion (for 8-bit encoding).
	 * @param  (\ReflectionFunctionAbstract|\ReflectionParameter|\ReflectionClass|\ReflectionProperty|string)[]  $possibilities
	 * @return string
	 *
	 * @author David Grudl
	 * @link https://github.com/nette/utils/blob/master/src/Utils/ObjectHelpers.php
	 */
	public static function getSuggestion(array $possibilities, string $value)
	{
		$norm = preg_replace($re = '#^(get|set|has|is|add)(?=[A-Z])#', '', $value);
		$best = null;
		$min = (strlen($value) / 4 + 1) * 10 + .1;
		foreach (array_unique($possibilities, SORT_REGULAR) as $item) {
			$item = $item instanceof \Reflector ? $item->getName() : $item;
			if ($item !== $value && (
				($len = levenshtein($item, $value, 10, 11, 10)) < $min
				|| ($len = levenshtein(preg_replace($re, '', $item), $norm, 10, 11, 10) + 20) < $min
			)) {
				$min = $len;
				$best = $item;
			}
		}
		return $best;
	}
}
