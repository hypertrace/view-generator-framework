package org.hypertrace.core.viewcreator.pinot;


import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.auth.NullAuthProvider;
import org.apache.pinot.common.auth.StaticTokenAuthProvider;
import org.apache.pinot.common.auth.UrlAuthProvider;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "AddSchema")
public class PinotAddSchemaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotAddSchemaCommand.class);
  static final String DEFAULT_CONTROLLER_PORT = "9000";

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "port name for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-schemaFile"}, required = true, description = "Path to schema file.")
  private String _schemaFile = null;

  @CommandLine.Option(names = {"-exec"}, required = false, description = "Execute the command.")
  private boolean _exec;

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;

  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  private String _authTokenUrl;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;

  private AuthProvider _authProvider;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String description() {
    return "Add schema specified in the schema file to the controller";
  }

  @Override
  public String getName() {
    return "AddSchema";
  }

  @Override
  public String toString() {
    String retString = ("AddSchema -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
        + " -controllerPort " + _controllerPort + " -schemaFile " + _schemaFile + " -user " + _user + " -password "
        + "[hidden]");

    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public void cleanup() {
  }

  public PinotAddSchemaCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public PinotAddSchemaCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public PinotAddSchemaCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public PinotAddSchemaCommand setSchemaFilePath(String schemaFilePath) {
    _schemaFile = schemaFilePath;
    return this;
  }

  public void setUser(String user) {
    _user = user;
  }

  public void setPassword(String password) {
    _password = password;
  }

  public void setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
  }

  public PinotAddSchemaCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    if (!_exec) {
      LOGGER.warn("Dry Running Command: " + toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    File schemaFile = new File(_schemaFile);
    LOGGER.info("Executing command: " + toString());
    if (!schemaFile.exists()) {
      throw new FileNotFoundException("file does not exist, + " + _schemaFile);
    }

    Schema schema = Schema.fromFile(schemaFile);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.addSchema(FileUploadDownloadClient
              .getUploadSchemaURI(_controllerProtocol, _controllerHost, Integer.parseInt(_controllerPort)),
          schema.getSchemaName(), schemaFile, AuthProviderUtils
              .toRequestHeaders(makeAuthProvider(_authProvider, _authTokenUrl, _authToken,
              _user, _password)), List.of(new BasicNameValuePair("force", "true")));
    } catch (Exception e) {
      LOGGER.error("Got Exception to upload Pinot Schema: " + schema.getSchemaName(), e);
      return false;
    }
    return true;
  }

  @Nullable
  static AuthProvider makeAuthProvider(AuthProvider provider, String tokenUrl, String authToken, String user,
      String password) {
    if (provider != null) {
      return provider;
    }

    if (StringUtils.isNotBlank(tokenUrl)) {
      return new UrlAuthProvider(tokenUrl);
    }

    if (StringUtils.isNotBlank(authToken)) {
      return new StaticTokenAuthProvider(authToken);
    }

    if (StringUtils.isNotBlank(user)) {
      return new StaticTokenAuthProvider(BasicAuthUtils.toBasicAuthToken(user, password));
    }

    return new NullAuthProvider();
  }
}
