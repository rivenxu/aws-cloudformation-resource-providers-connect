package software.amazon.connect.userhierarchygroup;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.connect.ConnectClient;
import software.amazon.awssdk.services.connect.model.TagResourceRequest;
import software.amazon.awssdk.services.connect.model.UntagResourceRequest;
import software.amazon.awssdk.services.connect.model.UpdateUserHierarchyGroupNameRequest;
import software.amazon.awssdk.services.connect.model.CreateUserHierarchyGroupRequest;
import software.amazon.awssdk.services.connect.model.DeleteUserHierarchyGroupRequest;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UpdateHandler extends BaseHandlerStd {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<ConnectClient> proxyClient,
        final Logger logger) {

        final ResourceModel desiredStateModel = request.getDesiredResourceState();
        final ResourceModel previousStateModel = request.getPreviousResourceState();
        final Set<Tag> previousResourceTags = convertResourceTagsToSet(request.getPreviousResourceTags());
        final Set<Tag> desiredResourceTags = convertResourceTagsToSet(request.getDesiredResourceTags());
        final Set<Tag> tagsToRemove = Sets.difference(previousResourceTags, desiredResourceTags);
        final Set<Tag> tagsToAdd = Sets.difference(desiredResourceTags, previousResourceTags);

        logger.log(String.format("Invoked UpdateUserHierarchyGroupHandler with UserHierarchyGroup:%s", desiredStateModel.getUserHierarchyGroupArn()));

        if (StringUtils.isNotEmpty(desiredStateModel.getInstanceArn()) && !desiredStateModel.getInstanceArn().equals(previousStateModel.getInstanceArn())) {
            throw new CfnInvalidRequestException("InstanceArn cannot be updated.");
        }

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> updateUserHierarchyGroupName(proxy, proxyClient, desiredStateModel, previousStateModel, progress, callbackContext, logger))
                .then(progress -> updateUserHierarchyGroupParent(proxy, proxyClient, desiredStateModel, previousStateModel, progress, callbackContext, logger))
//                .then(progress -> unTagResource(proxy, proxyClient, desiredStateModel, tagsToRemove, progress, callbackContext, logger))
//                .then(progress -> tagResource(proxy, proxyClient, desiredStateModel, tagsToAdd, progress, callbackContext, logger))
                .then(progress -> ProgressEvent.defaultSuccessHandler(desiredStateModel));
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateUserHierarchyGroupName(final AmazonWebServicesClientProxy proxy,
                                                                                 final ProxyClient<ConnectClient> proxyClient,
                                                                                 final ResourceModel desiredStateModel,
                                                                                 final ResourceModel previousStateModel,
                                                                                 final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                 final CallbackContext context,
                                                                                 final Logger logger) {

        final boolean updateUserHierarchyGroupName = !StringUtils.equals(desiredStateModel.getName(), previousStateModel.getName());

        if (updateUserHierarchyGroupName) {
            logger.log(String.format("Calling UpdateUserHierarchyGroupName API for UserHierarchyGroup:%s", desiredStateModel.getUserHierarchyGroupArn()));
            return proxy.initiate("connect::updateUserHierarchyGroupName", proxyClient, desiredStateModel, context)
                    .translateToServiceRequest(UpdateHandler::translateToUpdateUserHierarchyGroupNameRequest)
                    .makeServiceCall((req, clientProxy) -> invoke(req, clientProxy, clientProxy.client()::updateUserHierarchyGroupName, logger))
                    .progress();
        } else {
            logger.log(String.format("UserHierarchyGroup name field is unchanged from in the update operation, " +
                    "skipping UpdateUserHierarchyGroupName API call for UserHierarchyGroup:%s", desiredStateModel.getUserHierarchyGroupArn()));
            return progress;
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateUserHierarchyGroupParent(final AmazonWebServicesClientProxy proxy,
                                                                                       final ProxyClient<ConnectClient> proxyClient,
                                                                                       final ResourceModel desiredStateModel,
                                                                                       final ResourceModel previousStateModel,
                                                                                       final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                       final CallbackContext context,
                                                                                       final Logger logger) {
        final boolean updateUserHierarchyGroupParent = !StringUtils.equals(desiredStateModel.getParentGroupArn(), previousStateModel.getParentGroupArn());

        if (updateUserHierarchyGroupParent) {
            logger.log(String.format("Calling CreateUserHierarchyGroup API for UserHierarchyGroup:%s", desiredStateModel.getUserHierarchyGroupArn()));
            return proxy.initiate("connect::createUserHierarchyGroup", proxyClient, desiredStateModel, context)
                    .translateToServiceRequest(UpdateHandler::translateToCreateUserHierarchyGroupRequest)
                    .makeServiceCall((req, clientProxy) -> invoke(req, clientProxy, clientProxy.client()::createUserHierarchyGroup, logger))
                    .progress()
                    .then(p -> deleteUserHierarchyGroupParent(proxy, proxyClient, desiredStateModel, previousStateModel, progress, context, logger));
        } else {
            logger.log(String.format("UserHierarchyGroup parent group arn field is unchanged from in the update operation, " +
                    "skipping update UserHierarchyGroup parent group arn calls for UserHierarchyGroup:%s", desiredStateModel.getUserHierarchyGroupArn()));
            return progress;
        }
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteUserHierarchyGroupParent(final AmazonWebServicesClientProxy proxy,
                                                                                         final ProxyClient<ConnectClient> proxyClient,
                                                                                         final ResourceModel desiredStateModel,
                                                                                         final ResourceModel previousStateModel,
                                                                                         final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                                         final CallbackContext context,
                                                                                         final Logger logger) {

            logger.log(String.format("Calling DeleteUserHierarchyGroup API for UserHierarchyGroup:%s", desiredStateModel.getUserHierarchyGroupArn()));
            return proxy.initiate("connect::deleteUserHierarchyGroup", proxyClient, desiredStateModel, context)
                    .translateToServiceRequest(UpdateHandler::translateToDeleteUserHierarchyGroupRequest)
                    .makeServiceCall((req, clientProxy) -> invoke(req, clientProxy, clientProxy.client()::deleteUserHierarchyGroup, logger))
                    .progress();
    }

    private static UpdateUserHierarchyGroupNameRequest translateToUpdateUserHierarchyGroupNameRequest(final ResourceModel model) {
        return UpdateUserHierarchyGroupNameRequest
                .builder()
                .instanceId(model.getInstanceArn())
                .hierarchyGroupId(model.getUserHierarchyGroupArn())
                .name(model.getName())
                .build();
    }

    private static CreateUserHierarchyGroupRequest translateToCreateUserHierarchyGroupRequest(final ResourceModel model) {
        return CreateUserHierarchyGroupRequest
                .builder()
                .instanceId(model.getInstanceArn())
                .parentGroupId(model.getParentGroupArn())
                .name(model.getName())
                .build();
    }

    private static DeleteUserHierarchyGroupRequest translateToDeleteUserHierarchyGroupRequest(final ResourceModel model) {
        return DeleteUserHierarchyGroupRequest
                .builder()
                .instanceId(model.getInstanceArn())
                .hierarchyGroupId(model.getUserHierarchyGroupArn())
                .build();
    }

    private ProgressEvent<ResourceModel, CallbackContext> tagResource(final AmazonWebServicesClientProxy proxy,
                                                                      final ProxyClient<ConnectClient> proxyClient,
                                                                      final ResourceModel desiredStateModel,
                                                                      final Set<Tag> tagsToAdd,
                                                                      final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                      final CallbackContext context,
                                                                      final Logger logger) {
        final String userHierarchyGroupArn = desiredStateModel.getUserHierarchyGroupArn();

        if (tagsToAdd.size() > 0) {
            logger.log(String.format("Tags have been modified(addition/TagValue updated) in the update operation, " +
                    "Calling TagResource API for UserHierarchyGroup:%s", userHierarchyGroupArn));
            return proxy.initiate("connect::tagResource", proxyClient, desiredStateModel, context)
                    .translateToServiceRequest(desired -> translateToTagRequest(userHierarchyGroupArn, tagsToAdd))
                    .makeServiceCall((req, clientProxy) -> invoke(req, clientProxy, clientProxy.client()::tagResource, logger))
                    .done(response -> ProgressEvent.progress(desiredStateModel, context));
        }
        logger.log(String.format("No new tags or change in value for existing keys in update operation," +
                " skipping TagResource API call for UserHierarchyGroup:%s", userHierarchyGroupArn));
        return progress;
    }

    private ProgressEvent<ResourceModel, CallbackContext> unTagResource(final AmazonWebServicesClientProxy proxy,
                                                                        final ProxyClient<ConnectClient> proxyClient,
                                                                        final ResourceModel desiredStateModel,
                                                                        final Set<Tag> tagsToRemove,
                                                                        final ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                        final CallbackContext context,
                                                                        final Logger logger) {
        final String userHierarchyGroupArn = desiredStateModel.getUserHierarchyGroupArn();

        if (tagsToRemove.size() > 0) {
            logger.log(String.format("Tags have been removed in the update operation, " +
                    "Calling UnTagResource API for UserHierarchyGroup:%s", userHierarchyGroupArn));
            return proxy.initiate("connect::untagResource", proxyClient, desiredStateModel, context)
                    .translateToServiceRequest(desired -> translateToUntagRequest(userHierarchyGroupArn, tagsToRemove))
                    .makeServiceCall((req, clientProxy) -> invoke(req, clientProxy, clientProxy.client()::untagResource, logger))
                    .done(response -> ProgressEvent.progress(desiredStateModel, context));
        }
        logger.log(String.format("No removal of tags in update operation, skipping UnTagResource API call " +
                "for UserHierarchyGroup:%s", userHierarchyGroupArn));
        return progress;
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    private UntagResourceRequest translateToUntagRequest(final String userHierarchyArn, final Set<Tag> tags) {
        final Set<String> tagKeys = streamOfOrEmpty(tags).map(Tag::getKey).collect(Collectors.toSet());

        return UntagResourceRequest.builder()
                .resourceArn(userHierarchyArn)
                .tagKeys(tagKeys)
                .build();
    }

    private TagResourceRequest translateToTagRequest(final String userHierarchyArn, final Set<Tag> tags) {
        return TagResourceRequest.builder()
                .resourceArn(userHierarchyArn)
                .tags(translateTagsToSdk(tags))
                .build();
    }

    private Map<String, String> translateTagsToSdk(final Set<Tag> tags) {
        return tags.stream().collect(Collectors.toMap(Tag::getKey,
                Tag::getValue));
    }
}
