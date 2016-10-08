package jp.classmethod.aws.dynamodb;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Optional;

/**
 * @author Daisuke Miyamoto
 * @since #version#
 */
@ToString
@RequiredArgsConstructor
public class VersionCondition {

	public static VersionCondition of(Optional<Long> optVersion) {
		return optVersion.map(VersionCondition::new).orElse(null);
	}

	public static VersionCondition of(Long version) {
		return of(Optional.ofNullable(version));
	}


	@Getter
	private final long version;

}
