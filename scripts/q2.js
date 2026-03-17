var results = db.events.aggregate([
    { $match: { user_id: { $ne: null }, product_id: { $ne: null } } },
    {
        $group: {
            _id: { user_id: "$user_id", product_id: "$product_id" },
            category_code: { $first: "$category_code" },
            brand: { $first: "$brand" },
            score: {
                $sum: {
                    $switch: {
                        branches: [
                            { case: { $eq: ["$event_type", "view"] }, then: 1 },
                            { case: { $eq: ["$event_type", "cart"] }, then: 2 },
                            { case: { $eq: ["$event_type", "purchase"] }, then: 5 }
                        ],
                        default: 0
                    }
                }
            }
        }
    },
    { $sort: { score: -1 } },
    { $limit: 100 },
    {
        $project: {
            user_id: "$_id.user_id",
            product_id: "$_id.product_id",
            category_code: 1,
            brand: 1,
            total_score: "$score"
        }
    }
]);
printjson(results);
