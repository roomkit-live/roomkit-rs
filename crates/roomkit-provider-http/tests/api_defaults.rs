//! Defaults of the standalone provider URL validator.

use roomkit_provider_http::ValidatePublicUrlOptions;

#[test]
fn public_url_defaults_match_the_provider_contract() {
    let options = ValidatePublicUrlOptions::default();
    // roomkit.providers.validate_public_url(allowed_schemes) = ('http', 'https')
    assert_eq!(options.allowed_schemes, ["http", "https"]);
    // roomkit.providers.validate_public_url(resolve_dns) = True
    assert!(options.resolve_dns);
}
