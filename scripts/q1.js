var results = db.messages.aggregate([
    { 
        $match: { 
            is_purchased: 't'
        } 
    },
    {
        $lookup: {
            from: "campaigns",
            localField: "campaign_id",
            foreignField: "id",
            as: "campaign"
        }
    },
    { $unwind: "$campaign" },
    {
        $group: {
            _id: { 
                campaign_id: "$campaign_id", 
                channel: "$campaign.channel",
                campaign_type: "$campaign.campaign_type"
            },
            clients_purchased: { $addToSet: "$client_id" }
        }
    },
    {
        $project: {
            campaign_id: "$_id.campaign_id",
            campaign_type: "$_id.campaign_type",
            channel: "$_id.channel",
            clients_purchased: { $size: "$clients_purchased" }
        }
    },
    { $sort: { clients_purchased: -1 } },
    { $limit: 10 }
]);

printjson(results.toArray());
