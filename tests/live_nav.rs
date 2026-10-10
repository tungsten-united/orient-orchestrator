//! Against the live nav-api (opt in): the orchestrator's own calls, on the real Itnig map.
//!
//!   NAV_URL=https://nav-api-613464313064.europe-southwest1.run.app/api/v1 NAV_API_TOKEN=... \
//!   NAV_LIVE_IMAGE=path/to/a/venue.jpg cargo test --test live_nav -- --ignored

use orient_orchestrator::pipeline::{Ask, Caller, Pipeline, Vote, follow_vote, locate_vote};

#[tokio::test]
#[ignore = "calls the live nav-api: needs NAV_URL, NAV_API_TOKEN and NAV_LIVE_IMAGE"]
async fn should_locate_and_route_on_the_live_nav_api() {
    let get = |k: &str| std::env::var(k).ok();
    let p = Pipeline::from_vars(&get);
    let image = std::fs::read(get("NAV_LIVE_IMAGE").expect("NAV_LIVE_IMAGE")).unwrap();
    let caller = Caller {
        client: "live-test".into(),
        session: None,
        request: "live-1".into(),
    };
    let motion = serde_json::json!({"stepCount": 10, "headingDeg": null, "headingSource": null,
        "orientation": {"alpha": 0.0, "beta": 90.0, "gamma": 0.0, "absolute": true}});

    // Locating: a ranked answer the loop can vote on.
    let found = p
        .locate(
            vec![(Some(motion.clone()), image.clone())],
            &Ask::default(),
            &caller,
        )
        .await
        .expect("localize");
    assert!(["confirmed", "uncertain", "lost"].contains(&found["status"].as_str().unwrap()));
    assert!(found["candidates"].is_array() && found["margin"].is_number());
    let _ = locate_vote(&found, p.nav_loop.margin, p.nav_loop.locate_lost_margin);

    // One route from the entrance to the drinks area, then a localize on its first hop.
    let route = p.path("n1", "n2", &caller).await.expect("route");
    assert_eq!(route["found"], true);
    let hop = &route["hops"][0];
    assert_eq!(hop["source"], "n1");
    let output = p.validate(&route, "n1");
    assert!(["turn", "continue"].contains(&output.action.as_str()));
    let ask = Ask {
        previous: Some("n1".into()),
        expected: Some(hop["target"].as_str().unwrap().into()),
        previous_step_count: Some(0),
    };
    let found = p
        .locate(vec![(Some(motion), image)], &ask, &caller)
        .await
        .expect("localize on a hop");
    assert_eq!(found["expected"], hop["target"]);
    assert!(
        found["walked_m"].is_number(),
        "the motion reached nav-api: {found}"
    );
    let vote = follow_vote(
        &found,
        "n1",
        hop["target"].as_str().unwrap(),
        p.nav_loop.margin,
    );
    assert!(matches!(
        vote,
        Vote::Target | Vote::Elsewhere | Vote::Abstain
    ));
}
