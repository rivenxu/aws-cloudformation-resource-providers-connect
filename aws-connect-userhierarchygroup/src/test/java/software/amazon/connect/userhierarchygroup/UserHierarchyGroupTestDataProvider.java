package software.amazon.connect.userhierarchygroup;

public class UserHierarchyGroupTestDataProvider {
    protected static final String USER_HIERARCHY_GROUP_ARN = "arn:aws:connect:us-west-2:111111111111:instance/instanceId/agent-group/userhierarchygroupId";
    protected static final String USER_HIERARCHY_GROUP_ID = "userhierarchygroupId";
    protected static final String INSTANCE_ARN = "arn:aws:connect:us-west-2:111111111111:instance/instanceId";
    protected static final String USER_HIERARCHY_GROUP_NAME = "userhierarchygroupName";
    protected static final String PARENT_HIERARCHY_GROUP_ARN = "arn:aws:connect:us-west-2:111111111111:instance/instanceId/agent-group/parenthierarchygroupId";
    protected static final String PARENT_HIERARCHY_GROUP_ID = "parenthierarchygroupId";

    //    protected static HierarchyGroup getDescribeUserHierarchyGroupResponseObject() {
//
//    }
    protected static ResourceModel buildUserHierarchyGroupResourceModel() {
        return ResourceModel.builder()
                .instanceArn(INSTANCE_ARN)
                .name(USER_HIERARCHY_GROUP_NAME)
                .parentGroupArn(PARENT_HIERARCHY_GROUP_ARN)
                .userHierarchyGroupArn(USER_HIERARCHY_GROUP_ARN)
                .build();
    }
}
